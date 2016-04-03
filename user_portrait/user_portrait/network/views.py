#-*- coding:utf-8 -*-

import os
import json
from flask import Blueprint, url_for, render_template, request,\
                    abort, flash, session, redirect
from utils import submit_network_keywords

mod = Blueprint('network', __name__, url_prefix='/network')


#use to submit keywords network compute task to redis and es
@mod.route('/submit_network_keywords/')
def ajax_submit_network_keywords():
    start_date = request.args.get('start_date', '2013-09-01')
    end_date = request.args.get('end_date', '2013-09-07')
    keywords_string = request.args.get('keywords', 'test') #keywords_string=word1,word2
    submit_user = request.args.get('submit_user', 'admin')
    results = submit_network_keywords(keywords_string, start_date, end_date, submit_user)
    if not results:
        results = ''
    return json.dumps(results)




