# -*- coding: utf-8 -*-
from typing import Optional
from minerva.loading.csv.parser import Parser

from minerva.harvest.plugin_api_trend import HarvestPluginTrend


class Plugin(HarvestPluginTrend):
    @staticmethod
    def create_parser(config: Optional[dict]):
        """Return parser instance."""
        return Parser(config)
