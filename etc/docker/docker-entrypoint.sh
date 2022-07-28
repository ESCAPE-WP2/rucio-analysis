#!/bin/bash

export PYTHONWARNINGS='ignore:Unverified HTTPS request'

# Generate Rucio configuration file from template
echo "initialising Rucio"
/etc/profile.d/rucio_init.sh
echo

# Pass through kube config if set
if [ -v KUBE_CONFIG_VALUE ]
then
  echo "$KUBE_CONFIG_VALUE" > ~/.kube/config
fi

# Set up authentication
if [ "${RUCIO_CFG_AUTH_TYPE,,}" == 'userpass' ]
then
  if [ -v RUCIO_CFG_USERNAME ] && [ -v RUCIO_CFG_PASSWORD ]
  then
    echo "proceeding with userpass authentication..."
  else
    echo "requested userpass auth but one or more of \$RUCIO_CFG_USERNAME or \$RUCIO_CFG_PASSWORD are not set"
    echo "quitting"
    exit
  fi
elif [ "${RUCIO_CFG_AUTH_TYPE,,}" == 'x509' ]
then
  if [ -v RUCIO_CFG_CLIENT_CERT_VALUE ] && [ -v RUCIO_CFG_CLIENT_KEY_VALUE ] && [ -v VOMS ] # if certificate/key are being passed in as values (e.g. from a k8s secret)
  then
    echo "proceeding with X509 authentication via passed key/certificate values..."
    # copy in credentials
    echo "$RUCIO_CFG_CLIENT_CERT_VALUE" > "/opt/rucio/etc/usercert.pem"
    echo "$RUCIO_CFG_CLIENT_KEY_VALUE" > "/opt/rucio/etc/userkey.pem"
    chmod 600 "/opt/rucio/etc/usercert.pem"
    chmod 400 "/opt/rucio/etc/userkey.pem"
    # export Rucio X509 client credentials
    export RUCIO_CFG_CLIENT_CERT="/opt/rucio/etc/usercert.pem"
    export RUCIO_CFG_CLIENT_KEY="/opt/rucio/etc/userkey.pem"
  elif [ -v RUCIO_CFG_CLIENT_CERT ] && [ -v RUCIO_CFG_CLIENT_KEY ] && [ -v VOMS ] # if certificate/key are being passed in as paths (e.g. from volume binds)
  then
    echo "proceeding with X509 authentication via passed key/certificate paths..."
    echo "! make sure that that you have volume bound your certificate/key at the locations specified by RUCIO_CFG_CLIENT_CERT and RUCIO_CFG_CLIENT_KEY respectively !"
    echo
  else
    echo "requested X509 auth but one or more of \$RUCIO_CFG_CLIENT_CERT_*, \$RUCIO_CFG_CLIENT_KEY_* or \$VOMS are not set"
    exit
  fi
  # set X509 credentials for FTS client
  export X509_USER_KEY=$RUCIO_CFG_CLIENT_KEY
  export X509_USER_CERT=$RUCIO_CFG_CLIENT_CERT
  # create X509 user proxy
  voms-proxy-init --cert "$RUCIO_CFG_CLIENT_CERT" --key "$RUCIO_CFG_CLIENT_KEY" --voms "$VOMS"
elif [ "${RUCIO_CFG_AUTH_TYPE,,}" == 'oidc' ]
then
  if [ -v OIDC_AGENT_AUTH_CLIENT_CFG_VALUE ] && [ -v OIDC_AGENT_AUTH_CLIENT_CFG_PASSWORD ] && [ -v RUCIO_CFG_ACCOUNT ] # if client config is being passed in as a value (e.g. from a k8s secret)
  then
    echo "proceeding with oidc authentication via passed client values..."
    # initialise oidc-agent
    # n.b. this assumes that the configuration has a refresh token attached to it with infinite lifetime (!)
    eval "$(oidc-agent-service use)"
    mkdir ~/.oidc-agent
    # copy across the auth client configuration
    echo "$OIDC_AGENT_AUTH_CLIENT_CFG_VALUE" > ~/.oidc-agent/rucio-auth
    # add configuration to oidc-agent
    oidc-add --pw-env=OIDC_AGENT_AUTH_CLIENT_CFG_PASSWORD rucio-auth
    # get client_name from configuration and retrieve token from oidc-agent trimming any newlines
    export client_name=$(oidc-gen --pw-env=OIDC_AGENT_AUTH_CLIENT_CFG_PASSWORD -p rucio-auth | jq -r .name)
    oidc-token --aud "rucio https://wlcg.cern.ch/jwt/v1/any" $client_name > "/tmp/tmp_auth_token_for_account_$RUCIO_CFG_ACCOUNT"
  elif [ -v OIDC_ACCESS_TOKEN ] && [ -v RUCIO_CFG_ACCOUNT ] # if access token is being passed in directly
    echo "$OIDC_ACCESS_TOKEN" > "/tmp/tmp_auth_token_for_account_$RUCIO_CFG_ACCOUNT"
  then
    echo "proceeding with oidc authentication using an access token..."
  else
    echo "requested oidc auth but one or more of \$OIDC_AGENT_AUTH_CLIENT_CFG_VALUE, \$OIDC_AGENT_AUTH_CLIENT_CFG_PASSWORD, \$RUCIO_CFG_ACCOUNT or \$OIDC_ACCESS_TOKEN are not set"
    exit
  fi
  tr -d '\n' < "/tmp/tmp_auth_token_for_account_$RUCIO_CFG_ACCOUNT" > "/tmp/auth_token_for_account_$RUCIO_CFG_ACCOUNT"
  # move this token to the location expected by Rucio
  mkdir -p /tmp/user/.rucio_user/
  mv "/tmp/auth_token_for_account_$RUCIO_CFG_ACCOUNT" /tmp/user/.rucio_user/
fi

echo
rucio whoami
echo

# if task has came in as yaml, do template substitution & pipe the result to a file
if [ -v TASK_FILE_YAML ]
then
  echo "$TASK_FILE_YAML" > /tmp/task.yaml
  j2 /tmp/task.yaml > /tmp/task.yaml.j2
  export TASK_FILE_PATH=/tmp/task.yaml.j2
fi

python3 src/run.py -v -t "$TASK_FILE_PATH"
