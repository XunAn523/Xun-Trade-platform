# Read-only: emits diagnostics, never changes rules, services or profiles.
[CmdletBinding()]
param(
    [System.Net.IPAddress]$LocalAddress = '192.168.251.2',
    [ValidateRange(1, 65535)][int]$Port = 19443
)

$ErrorActionPreference = 'Stop'
$issues = [System.Collections.Generic.List[string]]::new()
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [Security.Principal.WindowsPrincipal]::new($identity)
$elevated = $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

function Test-ProbePort {
    param($Filter)
    if ([string]$Filter.Protocol -notin @('TCP', '6', 'Any', '256')) { return $false }
    foreach ($entry in @($Filter.LocalPort)) {
        foreach ($part in ([string]$entry -split ',')) {
            $part = $part.Trim()
            if ($part -eq 'Any' -or $part -eq [string]$Port) { return $true }
            if ($part -match '^(\d+)-(\d+)$') {
                if ([int]$Matches[1] -le $Port -and $Port -le [int]$Matches[2]) { return $true }
            }
        }
    }
    return $false
}

$addressRows = @()
$networkRows = @()
$profileRows = @()
$serviceRows = @()
$listenerRows = @()
$ruleRows = [System.Collections.Generic.List[object]]::new()
try {
    $addresses = @(Get-NetIPAddress -ErrorAction Stop -AddressFamily IPv4 -IPAddress $LocalAddress.IPAddressToString)
    $addressRows = @($addresses | Select-Object IPAddress, PrefixLength, InterfaceAlias, InterfaceIndex)
    foreach ($address in $addresses) {
        $networkRows += @(Get-NetConnectionProfile -ErrorAction Stop -InterfaceIndex $address.InterfaceIndex |
            Select-Object InterfaceAlias, InterfaceIndex, NetworkCategory, IPv4Connectivity)
    }
} catch { $issues.Add('INTERFACE_OR_NETWORK_PROFILE_READ_FAILED:' + $_.Exception.GetType().Name) }
try {
    $profileRows = @(Get-NetFirewallProfile -PolicyStore ActiveStore -ErrorAction Stop |
        Select-Object Name, Enabled, DefaultInboundAction, DefaultOutboundAction,
            AllowInboundRules, AllowLocalFirewallRules, DisabledInterfaceAliases)
} catch { $issues.Add('ACTIVE_PROFILE_READ_FAILED:' + $_.Exception.GetType().Name) }
try {
    $serviceRows = @(Get-Service -ErrorAction Stop -Name MpsSvc, BFE | Select-Object Name, Status, StartType)
} catch { $issues.Add('FIREWALL_SERVICE_READ_FAILED:' + $_.Exception.GetType().Name) }
try {
    $listenerRows = @(Get-NetTCPConnection -ErrorAction Stop -LocalAddress $LocalAddress.IPAddressToString -LocalPort $Port -State Listen |
        Select-Object LocalAddress, LocalPort, State, OwningProcess)
} catch { $issues.Add('LISTENER_READ_FAILED_OR_ABSENT:' + $_.Exception.GetType().Name) }
try {
    # Include Any, TCP, matching ranges, allow AND block rules. An exact-port
    # search misses program/service rules whose LocalPort is Any.
    $filters = @(Get-NetFirewallPortFilter -PolicyStore ActiveStore -ErrorAction Stop | Where-Object { Test-ProbePort $_ })
    foreach ($filter in $filters) {
        $rules = @(Get-NetFirewallRule -AssociatedNetFirewallPortFilter $filter -PolicyStore ActiveStore -ErrorAction Stop |
            Where-Object { [string]$_.Enabled -eq 'True' -and [string]$_.Direction -eq 'Inbound' })
        foreach ($rule in $rules) {
            try {
                $remote = $rule | Get-NetFirewallAddressFilter -PolicyStore ActiveStore -ErrorAction Stop
                $program = $rule | Get-NetFirewallApplicationFilter -PolicyStore ActiveStore -ErrorAction Stop
                $service = $rule | Get-NetFirewallServiceFilter -PolicyStore ActiveStore -ErrorAction Stop
                $interface = $rule | Get-NetFirewallInterfaceFilter -PolicyStore ActiveStore -ErrorAction Stop
                $interfaceType = $rule | Get-NetFirewallInterfaceTypeFilter -PolicyStore ActiveStore -ErrorAction Stop
                $security = $rule | Get-NetFirewallSecurityFilter -PolicyStore ActiveStore -ErrorAction Stop
                $ruleRows.Add([ordered]@{
                    name = $rule.Name; display_name = $rule.DisplayName
                    action = [string]$rule.Action; profiles = [string]$rule.Profile
                    policy_source_type = [string]$rule.PolicyStoreSourceType
                    primary_status = [string]$rule.PrimaryStatus
                    enforcement_status = [string]$rule.EnforcementStatus
                    protocol = [string]$filter.Protocol; local_port = @($filter.LocalPort)
                    local_address = @($remote.LocalAddress); remote_address = @($remote.RemoteAddress)
                    program = $program.Program; package = $program.Package
                    service = $service.Service; interface_alias = @($interface.InterfaceAlias)
                    interface_type = [string]$interfaceType.InterfaceType
                    authentication = [string]$security.Authentication
                    override_block_rules = [string]$security.OverrideBlockRules
                })
            } catch { $issues.Add('RULE_FILTER_READ_FAILED:' + $rule.Name) }
        }
    }
} catch { $issues.Add('ACTIVE_RULE_ENUMERATION_FAILED:' + $_.Exception.GetType().Name) }

[ordered]@{
    schema = 'TradeCoreQmtProbeFirewallInspectionV1'
    inspected_at_utc = [DateTime]::UtcNow.ToString('o')
    read_only = $true
    elevated = $elevated
    status = $(if ($elevated -and $issues.Count -eq 0) { 'COLLECTED_REQUIRES_REVIEW' } else { 'PARTIAL_REQUIRES_ADMIN_REVIEW' })
    local_address = $LocalAddress.IPAddressToString
    port = $Port
    addresses = $addressRows
    network_profiles = $networkRows
    firewall_profiles = $profileRows
    firewall_services = $serviceRows
    listeners = $listenerRows
    candidate_rules = @($ruleRows.ToArray())
    query_errors = @($issues.ToArray())
    interpretation = 'Candidates are not proof of effective access. Review enabled/current profiles, interface exclusions, address/program/service filters and policy enforcement together. Non-admin or failed queries cannot prove no applicable rule exists.'
} | ConvertTo-Json -Depth 8
