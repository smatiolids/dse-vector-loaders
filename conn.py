# Astra Connection (Based on cqlsession, from cassio library)
import os
from cassandra.cluster import (
    Cluster,
)
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import AddressTranslator


class StaticTranslator(AddressTranslator):
    """
    Returns the endpoint with no translation
    """

    def translate(self, addr):
        return "127.0.0.1"


def getCQLSession(mode='astra_db'):
    print('Initializing CQL Session')
    if mode == 'astra_db':
        cluster = Cluster(
            cloud={
                "secure_connect_bundle": os.environ["ASTRA_DB_SECURE_BUNDLE_PATH"],
            },
            auth_provider=PlainTextAuthProvider(
                os.environ["ASTRA_DB_CLIENT_ID"],
                os.environ["ASTRA_DB_CLIENT_SECRET"],
            ),
        )
        astraSession = cluster.connect()
        astraSession.row_factory = dict_factory
        print('Connected')
        return astraSession
    elif mode == 'dse':
        print(os.environ["DSE_NODES"])
        print(os.environ["DSE_USER"])

        profile = ExecutionProfile(
            load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
            request_timeout=15,
        )
        address_translator = StaticTranslator()

        auth_provider = PlainTextAuthProvider(
            username=os.environ["DSE_USER"], password=os.environ['DSE_PASS'])

        cluster = Cluster(os.environ["DSE_NODES"].split(','),
                          address_translator=address_translator,
                          connect_timeout=30,
                          auth_provider=auth_provider)
        dseSession = cluster.connect()
        return dseSession
    elif mode == 'local':
        cluster = Cluster()
        localSession = cluster.connect()
        return localSession
    else:
        raise ValueError('Unknown CQL Session mode')
