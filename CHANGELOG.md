# Changelog

## 1.3.0
 * Tables the credentials cannot access (HTTP 403 / AccessDenied / 401 / Forbidden) are excluded from catalog[#18](https://github.com/singer-io/tap-intacct/pull/18)

## 1.2.1
  * Upgrade python version to 3.12 [#20](https://github.com/singer-io/tap-intacct/pull/20)
  * Add mock-integration tests

## 1.2.0
 * Add forced-replication-method to catalog metadata [#15](https://github.com/singer-io/tap-intacct/pull/15)

## 1.1.0
 * Adds proxy AWS Account support [#13](https://github.com/singer-io/tap-intacct/pull/13)
 * Update libraries 

## 1.0.3
 * Updated the regex pattern in s3.py to fetch filename_*.csv [#11](https://github.com/singer-io/tap-intacct/pull/11)

## 1.0.2
 * Fixed the regex pattern in s3.py [#9](https://github.com/singer-io/tap-intacct/pull/9)

## 1.0.1
 * Fixed an "Access Denied" error because the tap did not assume a role
   before trying to access S3 [#3](https://github.com/singer-io/tap-intacct/pull/3)

## 1.0.0
 * Releasing from Beta --> GA
