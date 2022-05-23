# Change Log for Tapis Security Kernel

All notable changes to this project will be documented in this file.

Please find documentation here:
https://tapis.readthedocs.io/en/latest/technical/security.html

You may also reference live-docs based on the openapi specification here:
https://tapis-project.github.io/live-docs

-----------------------

## 1.2 - 2022-05-31

Maintenance release

### Breaking Changes:
- Add grantor to create share request.

### New features:

### Bug fixes:
1. Fix syntax error in generated SQL clause on create share. 

-----------------------

## 1.1.3 - 2022-05-09

SK Sharing APIs

### Breaking Changes:
- none.

### New features:
1. Adjust JVM memory options and other deployment file clean up.
2. Improve JWT validation and authentication logging.
3. Implement new SK Sharing APIs including required DB schema changes.

-----------------------

## 1.1.2 - 2022-03-04

Java 17 upgrade

### Breaking Changes:
- none.

### New features:
1. Upgrade code and images to Java 17.0.2.

-----------------------

## 1.1.1 - 2022-02-01

No changes

-----------------------

## 1.1.0 - 2022-01-07

New minor release, no changes.

-----------------------

## 1.0.5 - 2021-12-09

Bug fix release

### Breaking Changes:
- none.

### New features:
1. Improved getUserPerm endpoint livedocs documentation.

### Bug fixes:
1. Removed duplicate code.

-----------------------

## 1.0.3 - 2021-10-12

Bug fix release

### Breaking Changes:
- none.

### Bug fixes:
1. Fix version in non-standard POM files.

-----------------------

## 1.0.0 - 2021-07-16

Initial release supporting basic CRUD operations on Tapis authorization 
and secrets resources.

1. Authorization engine based on an extension of Apache Shiro semantics.
2. Authorization API for roles, permissions and users.
3. Secret support using Hashicorp Vault backend.
4. Secrets API with custom handling of system, database credentials, JWT signing keys
   service password and user secret types.

### Breaking Changes:
- Initial release.

### New features:
 - Initial release.

### Bug fixes:
- None.
