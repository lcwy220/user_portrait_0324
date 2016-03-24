# -*- coding:utf-8 -*-
import json
from flask import Blueprint, url_for, render_template, request, abort, flash, session, redirect
from utils import get_user_operation

from user_portrait.global_utils import es_user_portrait

mod = Blueprint('ucenter', __name__, url_prefix='/ucenter')

@mod.route('/user_operation/')
def ajax_user_operation():
    submit_user = request.args.get('submit_user', 'admin')
    results = get_user_operation(submit_user)
    if not results:
        results = {}
    return json.dumps(results)
