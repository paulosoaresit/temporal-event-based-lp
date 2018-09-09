#!/usr/bin/env python
# encoding: utf-8
"""
Author: Paulo R S Soares

The scripts contained in this file are responsible for pushing data from arXiv website by using its REST API. 
"""

import urllib.request
import feedparser
import unicodedata
import time
import dateutil
import sys
import networkx as nx
import difflib as diff
import pandas as pd
from datetime import datetime
from itertools import combinations
from progress.bar import Bar

ARXIV_SEARCH_QUERY_URI = 'http://export.arxiv.org/api/query?search_query=submittedDate:[%s01010000+TO+%s12312359]+AND+cat:%s&start=%i&max_results=%i'
MAX_RESULTS_PER_REQUEST = 200  # Max papers per request
WAIT_TIME = 3                  # Number of seconds to wait beetween requests

def retrieve_data_from(category, year_from, year_to):
    print('\nSearching arXiv papers from %s category...' % (category))

    for year in range(year_from, year_to + 1):
        print('Retrieving papers from year: %i...' % year)

        # Build a simple seach query with 1 paper request to retrieve info from the feed metadata
        query = (ARXIV_SEARCH_QUERY_URI % (year, year, category, 0, 1))

        # Perform a GET request and retrieve from the feed the total amount of data avaliable in the year
        response = urllib.request.urlopen(query)
        feed = feedparser.parse(response)
        total_results = int(feed.feed.opensearch_totalresults)
        print("Total: %i papers." % total_results)

        # Create a graph to keep the data
        g = nx.Graph()

        start_indexes = range(0, total_results, MAX_RESULTS_PER_REQUEST)
        bar = Bar('Pulling data', max=len(start_indexes), suffix='%(percent)d%%') # set up progress bar
        bar.start()
        for start_from in start_indexes:
            query = (ARXIV_SEARCH_QUERY_URI % (year, year, category, start_from, MAX_RESULTS_PER_REQUEST))
            response = urllib.request.urlopen(query)
            feed = feedparser.parse(response)
            parse_feed(feed, g)
            time.sleep(WAIT_TIME) # Avoid flooding the arXiv server

            # Update progress
            bar.next()

        bar.finish()

        print("Saving data to file...")
        save_as_csv(g, category, year)
        print('-' * 40)

def parse_feed(feed, g):
    for entry in feed.entries:
        # Papers with no authors are spurious data
        if entry.authors != None:
            # Paper partition
            paper_id = entry.id.split('/abs/')[-1]
            title = unicodedata.normalize('NFKD', entry.title)
            summary = unicodedata.normalize('NFKD', entry.summary)
            publish_date = dateutil.parser.parse(entry.published)  
            g.add_node(paper_id, bipartite='paper', title=title, summary=summary, publish_date=publish_date)

            # Author partition		
            for author in entry.authors: 
                # Mitigate typos in the arXiv repository by considering authors with similar names to be the same person               
                author_name = unicodedata.normalize('NFKD', author.name)
                similar_names = diff.get_close_matches(author_name, list(g.nodes()), n = 1)

                if similar_names:
                    author_name = similar_names[0]
                
                # Add the author and the existing link with the paper to the bipartite graph
                g.add_node(author_name, bipartite='author')
                g.add_edge(author_name, paper_id)

def save_as_csv(g, category, year):
    # Create node list
    node_list = []

    for id, data in g.nodes(data=True):
        node_data = dict()
        node_data['id'] = id
        node_data.update(data)        
        node_list.append(node_data)
    
    filename = '../datasets/%s/nodelist-%s-%i.csv' % (category, category, year)
    pd.DataFrame(node_list).to_csv(filename)

    # Create edge list
    edge_list = []

    for node1, node2, data in g.edges(data=True):
        edge_data = dict()
        edge_data['node1'] = node1
        edge_data['node2'] = node2
        edge_data.update(data)
        edge_list.append(edge_data)
    
    filename = '../datasets/%s/edgelist-%s-%i.csv' % (category, category, year)
    pd.DataFrame(edge_list).to_csv(filename)


    
