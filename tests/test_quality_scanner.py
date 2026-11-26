from pathlib import Path

from tools.quality.check_sensitive_content import scan_text


def test_scanner_distinguishes_literal_credentials_from_variable_references():
    literal = "password" + ' = "' + "do-not-commit-abcdef12345" + '"'
    assert scan_text(path=Path("sample.py"), text=literal)
    assert not scan_text(path=Path("sample.py"), text="password = session_account.password")
    assert not scan_text(path=Path("sample.py"), text='password = _record_text(record, "password")')


def test_json_keys_and_unquoted_environment_credentials_are_still_detected():
    key, value = "password", "private-generated-12345"
    assert scan_text(path=Path("sample.json"), text=f'{{"{key}": "{value}"}}')
    assert scan_text(path=Path(".env"), text=f"{key}={value}")
