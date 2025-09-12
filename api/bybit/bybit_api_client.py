#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bybit API Client
================
Simple wrapper for Bybit API v5
"""

from pybit.unified_trading import HTTP
import logging

logger = logging.getLogger(__name__)


def get_bybit_client(api_key: str = None, api_secret: str = None, testnet: bool = False) -> HTTP:
    """
    Create and return Bybit HTTP client

    Args:
        api_key: API key (optional for public endpoints)
        api_secret: API secret (optional for public endpoints)
        testnet: Use testnet endpoint

    Returns:
        HTTP: Bybit API client
    """
    if testnet:
        logger.info("Using Bybit testnet")
        return HTTP(testnet=True, api_key=api_key, api_secret=api_secret)
    else:
        logger.info("Using Bybit mainnet")
        return HTTP(testnet=False, api_key=api_key, api_secret=api_secret)
