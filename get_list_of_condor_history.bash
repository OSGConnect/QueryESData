#!/bin/bash
curl -XGET 'uct2-es-head.mwt2.org:9200/_cat/indices?v' | grep "condor_history-2016.2"
