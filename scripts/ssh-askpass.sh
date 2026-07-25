#!/bin/sh

# OpenSSH invokes this helper when manual deployment uses DEPLOY_PASSWORD.
if [ -z "${DEPLOY_PASSWORD:-}" ]; then
  exit 1
fi

printf '%s\n' "${DEPLOY_PASSWORD}"
