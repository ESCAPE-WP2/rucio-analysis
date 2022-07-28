import json
import os
import requests

from rucio.client.client import Client
from rucio.common.exception import AccountNotFound, Duplicate, RucioException

from tasks.task import Task


class SyncIAMRucio(Task):
    """ Sync users of an IAM instance to Rucio. """

    def __init__(self, logger):
        super().__init__(logger)
        self.iam_server_base_url = None
        self.admin_client_id = None
        self.admin_client_secret = None
        self.rucio_admin_iam_groups = None
        self.rucio_user_iam_groups = None
        self.rse_quota = None
        self.skip_accounts = None
        self.dry_run = True

    def get_list_of_users_from_IAM(self, token):
        """ Queries the IAM client for users. """
        start_index = 1
        count = 100
        header = {
            "Authorization": "Bearer {}".format(token)
        }

        users = []
        users_so_far = 0
        while True:
            response = requests.get(os.path.join(self.iam_server_base_url, "scim/Users"), headers=header,
                                                 params={"startIndex": start_index, "count": count})
            response = json.loads(response.text)

            users += response['Resources']
            users_so_far += response['itemsPerPage']

            if users_so_far < response['totalResults']:
                start_index += count
            else:
                break
        return users

    def _get_access_token(self):
        """ Authenticates with an iam client via a client_credentials grant and returns an access token. """
        request_data = {
            "client_id": self.admin_client_id,
            "client_secret": self.admin_client_secret,
            "grant_type": "client_credentials",
            "username": "",
            "password": "",
            "scope": "scim:read"                        # must have scim:read scope on client to read IAM users
        }
        r = requests.post(os.path.join(self.iam_server_base_url, "token"), data=request_data)
        response = json.loads(r.text)
        if 'access_token' not in response:
            raise RuntimeError("Authentication Failed")
        return response['access_token']

    def _parse_iam_users(self, users):
        """ Parse response from IAM scim/Users endpoint. """
        users_iam = []
        for user in users:
            username = user['userName']
            active = user['active']
            email = user['emails'][0]['value']
            userid = user['id']
            groups = []
            if 'groups' in user:
                for group in user['groups']:
                    groups.append(group['display'])
            users_iam.append({
                'username': username,
                'active': active,
                'email': email,
                'id': userid,
                'groups': groups,
            })
        return users_iam

    def add_oidc(self, users):
        self.logger.info("Updating OIDC identities...")

        # Get accounts for both IAM and Rucio.
        users_iam = self._parse_iam_users(users)

        rucio = Client()
        users_rucio = list(rucio.list_accounts())

        # Try find a Rucio account with a matching username. Add an OIDC identity if found.
        #
        for idx, account in enumerate(users_rucio):
            if account['account'] in self.skip_accounts:
                continue
            iam_user = [entry for entry in users_iam if entry['username'] == account['account']]
            if iam_user:
                iam_user = iam_user[0]
                user_identity = "SUB={}, ISS={}".format(iam_user['id'], self.iam_server_base_url)
                existing_identities = [identity for identity in rucio.list_identities(account['account']) \
                                       if identity['identity'] == user_identity and identity['type'] == 'OIDC']
                if not existing_identities:
                    self.logger.info('({}/{}) Adding OIDC identity for user {}'.format(
                        idx + 1, len(users_rucio), account['account']))
                    if not self.dry_run:
                        rucio.add_identity(account=account['account'], identity=user_identity, authtype='OIDC',
                                           default=True, email=iam_user['email'])
                else:
                    self.logger.info('({}/{}) Skipping adding OIDC identity for user {} [exists]'.format(
                        idx + 1, len(users_rucio), account['account']))
            else:
                self.logger.info('({}/{}) Skipping adding OIDC identity for user [does not exist in IAM]'.format(
                    idx + 1, len(users_rucio), account['account']))

    def sync_accounts(self, users):
        # Get accounts for both IAM and Rucio.
        #
        users_iam = self._parse_iam_users(users)

        rucio = Client()
        users_rucio = list(rucio.list_accounts())

        # First, compare the list of IAM users with existing Rucio accounts and add/delete accordingly.
        # This deals with the following cases:
        #
        # - User in Rucio but not IAM, in which case delete
        # - User in IAM but not in Rucio (or disabled), in which case (pending some checks)
        #   - Reactivate account (if user account already exists)
        #   - Add account (if user account does not exist)
        #
        # Accounts are added as either USER or SERVICE types depending on group membership (rucio_user_iam_groups and
        # rucio_admin_iam_groups respectively).
        #
        self.logger.info("Adding/removing/reactivating accounts...")

        # The following is the not intersection (XOR) of the two user lists from IAM and Rucio.
        accounts_not_intersect = set([entry['account'] for entry in users_rucio]) ^ \
                                 set([entry['username'] for entry in users_iam])
        for user in self.skip_accounts:
            accounts_not_intersect.remove(user)

        for idx, account in enumerate(accounts_not_intersect):
            if account in [account['account'] for account in users_rucio]:  # user in Rucio but not in IAM
                self.logger.info('({}/{}) Deleting account for user {} [not in IAM]'.format(
                    idx+1, len(accounts_not_intersect), account))
                user = [entry for entry in users_rucio if entry['account'] == account][0]
                if not self.dry_run:
                    try:
                        rucio.delete_account(user['account'])
                    except AccountNotFound:
                        pass
            else:                                                           # user in IAM but not in Rucio (or disabled)
                user = [entry for entry in users_iam if entry['username'] == account][0]

                # Skip user if account marked as inactive by IAM.
                if not user['active']:
                    self.logger.info('({}/{}) Skipped account creation for user {} [not active]'.format(
                        idx+1, len(accounts_not_intersect), user['username']))
                    continue

                # Skip user if account name violates Rucio database/schema restrictions.
                # a) must be <= 25 characters
                if len(user['username']) > 25:
                    self.logger.info('({}/{}) Skipped account creation for user {} [len(account) > 25]'.format(
                        idx+1, len(accounts_not_intersect), user['username']))
                    continue
                # a) must not contain @
                if '@' in user['username']:
                    self.logger.info('({}/{}) Skipped account creation for user {} [contains @]'.format(
                        idx + 1, len(accounts_not_intersect), user['username']))
                    continue

                try:  # Check if user account already exists in Rucio and is disabled...
                    account_status = rucio.get_account(user['username'])['status']
                    # account exists
                    if set(user['groups']).intersection(self.rucio_admin_iam_groups) or \
                        set(user['groups']).intersection(self.rucio_user_iam_groups):       # verified as admin or user
                        if account_status == 'ACTIVE':
                            self.logger.info('({}/{}) Skipped account for user {} [already exists & active]'.format(
                                idx+1, len(accounts_not_intersect), user['username']))
                        elif account_status == 'DELETED':
                            self.logger.info('({}/{}) Setting account from DELETED to ACTIVE for user {} '.format(
                                idx + 1, len(accounts_not_intersect), user['username']))
                            rucio.update_account(user['username'], 'status', 'ACTIVE')
                    else:
                        self.logger.info('({}/{}) Skipped account reactivation for existing user {} [not a member of '
                                         'any required groups]'.format(
                            idx + 1, len(accounts_not_intersect), user['username']))
                except AccountNotFound:  # ...if not, add an account
                    if set(user['groups']).intersection(self.rucio_admin_iam_groups):        # verified as admin
                        if not self.dry_run:
                            self.logger.info('({}/{}) Creating service account for {}'.format(
                                idx + 1, len(accounts_not_intersect), user['username']))
                            rucio.add_account(user['username'], type_="SERVICE", email=user['email'])
                    elif set(user['groups']).intersection(self.rucio_user_iam_groups):       # verified as user
                        self.logger.info('({}/{}) Creating user account for {}'.format(
                            idx + 1, len(accounts_not_intersect), user['username']))
                        if not self.dry_run:
                            rucio.add_account(user['username'], type_="USER", email=user['email'])
                    else:
                        self.logger.info('({}/{}) Skipped account creation for user {} [not a member of required '
                                         'groups]'.format(
                                            idx + 1, len(accounts_not_intersect), user['username']))

        users_rucio = list(rucio.list_accounts())   # refresh

        # Now consider all existing accounts and update accordingly
        #
        # This does the following:
        #
        # - Syncs account_types
        # - Syncs account attributes for users e.g. sign-gcs
        #   - Assigns rse quotas
        # - Syncs account attributes for admins e.g. admin
        #
        # Accounts are updated as either USER or SERVICE types depending on group membership (rucio_user_iam_groups and
        # rucio_admin_iam_groups respectively).
        #
        self.logger.info("Updating accounts...")
        for idx, account in enumerate(users_rucio):
            if account['account'] in self.skip_accounts:
                continue
            self.logger.info('({}/{}) Considering changes for user {}'.format(
                idx + 1, len(users_rucio), account['account']))
            if not self.dry_run:
                try:
                    #rucio_user = [entry for entry in users_iam if entry['account'] == account][0]
                    iam_user = [entry for entry in users_iam if entry['username'] == account['account']][0]
                except IndexError:
                    continue
                attributes = list(rucio.list_account_attributes(account['account']))[0]

                # sync account type
                account_type = rucio.get_account(account['account'])['account_type']
                if set(iam_user['groups']).intersection(self.rucio_admin_iam_groups):   # verified as admin
                    if account_type != 'SERVICE':
                        self.logger.debug(" -> Updating account_type to SERVICE")
                        rucio.update_account(account['account'], 'account_type', 'SERVICE')
                elif set(iam_user['groups']).intersection(self.rucio_user_iam_groups):  # verified as user
                    if account_type != 'USER':
                        self.logger.debug(" -> Updating account_type to USER")
                        rucio.update_account(account['account'], 'account_type', 'USER')
                else:                                                                   # not a member of any groups
                    rucio.delete_account(account['account'])

                # sync attributes etc. for a user belonging to a valid user group
                if set(iam_user['groups']).intersection(self.rucio_user_iam_groups):    # verified as user
                    # attr: sign-gcs
                    exists = [entry for entry in attributes if entry['key'] == 'sign-gcs']
                    if exists:
                        if not exists[0]['value']:
                            self.logger.debug(" -> Removing incorrect sign-gcs attribute")
                            rucio.delete_account_attribute(account['account'], 'sign-gcs')
                            exists = False
                    if not exists:
                        self.logger.debug(" -> Adding sign-gcs account attribute")
                        rucio.add_account_attribute(account['account'], 'sign-gcs', 'True')

                    # assign fixed quota for all RSEs to account
                    for rse in rucio.list_rses():
                        try:
                            limit = rucio.get_local_account_limits(account['account'])[rse['rse']]
                            if limit == self.rse_quota:
                                continue
                        except KeyError:
                            pass
                        self.logger.debug(" -> Adding quota {} for account {} at rse {}".format(
                            self.rse_quota, account['account'], rse['rse']))
                        rucio.set_local_account_limit(account['account'], rse['rse'], self.rse_quota)
                else:                                                                   # not verified as a user
                    exists = [entry for entry in attributes if entry['key'] == 'sign-gcs']
                    if exists:
                        self.logger.debug(" -> Deleting sign-gcs account attribute")
                        try:
                            rucio.delete_account_attribute(account['account'], 'sign-gcs')
                        except AccountNotFound:
                            pass

                # sync attributes etc. for a user belonging to the admin group
                if set(iam_user['groups']).intersection(self.rucio_admin_iam_groups):  # verified as admin
                    # attr: admin
                    exists = [entry for entry in attributes if entry['key'] == 'admin']
                    if exists:
                        if not exists[0]['value']:
                            self.logger.debug(" -> Removing incorrect admin attribute")
                            rucio.delete_account_attribute(account['account'], 'admin')
                            exists = False
                    if not exists:
                        self.logger.debug(" -> Adding admin account attribute")
                        rucio.add_account_attribute(account['account'], 'admin', 'True')
                else:                                                                   # not verified as an admin
                    exists = [entry for entry in attributes if entry['key'] == 'admin']
                    if exists:
                        self.logger.debug(" -> Deleting admin account attribute")
                        try:
                            rucio.delete_account_attribute(account['account'], 'admin')
                        except AccountNotFound:
                            pass

    def run(self, args, kwargs):
        super().run()
        self.tic()
        try:
            # Assign variables from tests.stubs.yml kwargs.
            #
            self.iam_server_base_url = kwargs['iam_server_base_url']
            self.admin_client_id = kwargs['client_id']
            self.admin_client_secret = kwargs['client_secret']
            self.rucio_admin_iam_groups = kwargs['rucio_admin_iam_groups']
            self.rucio_user_iam_groups = kwargs['rucio_user_iam_groups']
            self.rse_quota = kwargs['rse_quota']
            self.skip_accounts = kwargs['skip_accounts']
            self.dry_run = kwargs['dry_run']
        except KeyError as e:
            self.logger.critical("Could not find necessary kwarg for task.")
            self.logger.critical(repr(e))
            return False

        self.logger.info("Starting IAM -> Rucio synchronisation.")

        # get access token
        access_token = self._get_access_token()

        # get list of all users from IAM
        iam_users = self.get_list_of_users_from_IAM(access_token)

        # sync accounts and add oidc identities
        self.sync_accounts(iam_users)
        self.add_oidc(iam_users)

        self.logger.info("IAM -> Rucio synchronisation completed successfully.")

        self.toc()
        self.logger.info("Finished in {}s".format(
            round(self.elapsed)))
