class APIRateLimitError(Exception):
    """Exceção levantada quando a API retorna erro 429 (Too Many Requests)."""

    pass


class APIConnectionError(Exception):
    """Exceção levantada quando a API retorna erros de conexão como 400(Bad Request), 401(Unauthorized), 403(Forbidden), etc."""

    pass
