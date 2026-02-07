# Security Tests

This directory contains reproduction tests for security findings. All fixes must be preceded by a failing test here.

## Conventions
- Test names: `test_exploit_<id>.py`
- Each test should include:
  - Description of the exploit vector
  - Expected failure mode (fail-closed behavior)
  - Minimal inputs to reproduce

## Workflow
1. Write a failing test in `tests/security/` that reproduces the issue.
2. Open a GitHub issue using the security issue template.
3. Implement the minimal fix.
4. Re-run security + baseline tests.
5. Update vulnerability register.
