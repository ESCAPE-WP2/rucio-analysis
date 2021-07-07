from rucio.client.subscriptionclient import SubscriptionClient
from rucio.common.exception import RucioException


class RucioAdminWrappersAPI:
    """ Talk to a Rucio instance via the admin API. """

    @staticmethod
    def addSubscription(*args, **kwargs):
        """
        Adds a new subscription which will be verified against every new added file and
        dataset
            :param name: Name of the subscription
                    :type:  String
            :param account: Account identifier
                    :type account:  String
            :param filter: Dictionary of attributes by which the input data should be
                filtered
                        **Example**: ``{'project': 'project1', 'account': 'tzero'}``
                    :type filter:  Dict
            :param replication_rules: Replication rules to be set : Dictionary with keys
                copies, rse_expression, weight, rse_expression
                    :type replication_rules:  Dict
            :param comments: Comments for the subscription
                    :type comments:  String
            :param lifetime: Subscription's lifetime (days); False if subscription has
                no lifetime
                    :type lifetime:  Integer or False
            :param retroactive: Flag to know if the subscription should be applied on
                previous data
                    :type retroactive:  Boolean
            :param dry_run: Just print the subscriptions actions without actually
                executing them (Useful if retroactive flag is set)
                    :type dry_run:  Boolean
            :param priority: The priority of the subscription (3 by default)
                    :type priority: Integer
        """
        try:
            client = SubscriptionClient()
            return client.add_subscription(*args, **kwargs)
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def listSubscriptions(*args, **kwargs):
        """
        Returns a dictionary with the subscription information :
        Examples: ``{'status': 'INACTIVE/ACTIVE/BROKEN', 'last_modified_date': ...}``
            :param name: Name of the subscription
                    :type:  String
            :param account: Account identifier
                    :type account:  String
            :returns: Dictionary containing subscription parameter
                    :rtype:   Dict
            :raises: exception.NotFound if subscription is not found
        """
        try:
            client = SubscriptionClient()
            return client.list_subscriptions(*args, **kwargs)
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def updateSubscription(*args, **kwargs):
        """
        Updates a subscription - args same as for addSubscription
        """
        try:
            client = SubscriptionClient()
            return client.update_subscription(*args, **kwargs)
        except RucioException as error:
            raise Exception(error)

    @staticmethod
    def listSubscriptionRules(*args, **kwargs):
        """
        List the associated rules of a subscription.
            :param account: Account of the subscription.
            :param name:    Name of the subscription.
        """
        try:
            client = SubscriptionClient()
            return client.list_subscription_rules(*args, **kwargs)
        except RucioException as error:
            raise Exception(error)
