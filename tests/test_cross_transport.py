"""Cross-transport conformance foundation tests.

Validates that REST and GraphQL transports can be imported and constructed.
Live backend3 conformance is opt-in via TUPAGO_BACKEND3_LAB=1.
"""

from tupago3_sdk.graphql_transport import GraphQlPaymentTransport
from tupago3_sdk.rest_transport import RestPaymentTransport


def test_rest_transport_can_be_constructed():
    t = RestPaymentTransport("http://127.0.0.1:8080")
    assert t is not None
    t.close()


def test_graphql_transport_can_be_constructed():
    t = GraphQlPaymentTransport("http://127.0.0.1:8080")
    assert t is not None
    t.close()


def test_rest_has_canonical_methods():
    t = RestPaymentTransport("http://127.0.0.1:8080")
    assert hasattr(t, "create_qr")
    assert hasattr(t, "create_telefonico")
    assert hasattr(t, "get_qr_status")
    assert hasattr(t, "get_telefonico_status")
    assert hasattr(t, "get_cliente")
    assert hasattr(t, "get_catalogo")
    t.close()


def test_graphql_has_canonical_methods():
    t = GraphQlPaymentTransport("http://127.0.0.1:8080")
    assert hasattr(t, "create_qr")
    assert hasattr(t, "create_telefonico")
    assert hasattr(t, "get_qr_status")
    assert hasattr(t, "get_telefonico_status")
    t.close()
