from os import stat

from django.http.response import StreamingHttpResponse
import facebook10
import requests
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import time
from dateutil import parser
from keywrd import keyword_mapping
import pymongo
from sqlalchemy import create_engine
from urllib.parse import quote_plus


def get_graph_api(user_token=None):
    access_token = '361427747864031|ebfhhjHF_qApg8jWM9KWkmo-sAw'
    if user_token:
        access_token = user_token
    graph = facebook10.GraphAPI(access_token, version='10.0')
    return graph


def get_mongodb_client(user, password, database):
    usermongo = quote_plus(user)
    passwd = quote_plus(password)
    dbname = quote_plus(database)
    client = MongoClient(
        "mongodb+srv://" + usermongo + ":" + passwd + "@intuaition-3.5n9rv.mongodb.net/" + dbname + "?retryWrites=true&w=majority")
    return client.psa


def get_ids(username, user_id, project_id, agency_id, org_id, task_id):
    database = 'db_analysisrelated'
    host = 'db-social-qa.mysql.database.azure.com'
    password = 'Gc96%40dh3'
    user = 'socialadminqa%40db-social-qa.mysql.database.azure.com'
    
    engine_args = {'ssl': {'fake_flag_to_enable_tls': False}}
    engine = create_engine('mysql+pymysql://{}:{}@{}/{}'.format(user, password, host, database),
                           connect_args=engine_args)
    
    con = engine.connect()
    
    user_meta = pd.read_sql('select * from partition_meta_users', con)
    project_meta = pd.read_sql('select * from partition_meta_projects', con)
    screenname_meta = pd.read_sql('select * from partition_meta_screennames where source="facebook"', con)
    
    user_meta = user_meta[user_meta['user_id'] == int(user_id)]
    if user_meta.empty:
        return {'55': 'User ID ' + user_id + ' not present in user meta table'}, None
    else:
        user_meta = user_meta[user_meta['org_id'] == int(org_id)]
        if user_meta.empty:
            return {'55': 'Org ID ' + org_id + ' not present in user meta table'}, None
        else:
            user_meta = user_meta[user_meta['agency_id'] == int(agency_id)]
            if user_meta.empty:
                return {'55': 'Agency ID ' + agency_id + ' not present in user meta table'}, None
    
    project_meta = project_meta[project_meta['project_id'] == int(project_id)]
    if project_meta.empty:
        return {'55': 'Project ID ' + project_id + ' not present in project meta table'}, None
    else:
        project_meta = project_meta[project_meta['task_id'] == task_id]
        if project_meta.empty:
            return {'55': 'Task ID ' + task_id + ' not present in project meta table'}, None
    
    screenname_meta = screenname_meta[screenname_meta['screen_name'] == username]
    if screenname_meta.empty:
        return {'55': 'Facebook ' + username + ' not present in screen_name meta table'}, None
    
    u_id = int(user_meta['u_id'].values[0])
    p_id = int(project_meta['p_id'].values[0])
    s_id = int(screenname_meta['s_id'].values[0])
    
    return None, (u_id, p_id, s_id)


def check_username(username, user_status):
    graph = get_graph_api()
    if user_status == '1':
        try:
            profile = graph.get_object(username)
            if profile and 'id' in profile:
                return {'200': 'Valid Username'}
            else:
                return {'400': 'Invalid Username'}
        except:
            return {'400': 'Invalid Username'}
    else:
        return {'8': 'User is not Permitted to Perform the Operation'}


def search_page(query):
    graph = get_graph_api()
    pages = graph.get_connections('pages', 'search?q=' + query)
    results = []
    if pages and "data" in pages:
        for page in pages["data"]:
            result = graph.get_connections(page['id'], '?fields=id,name,username,category,picture,followers_count,fan_count')
            results.append(result)
    return results


def get_profile_posts(username, graph, start_date, end_date):
    profile = graph.get_object(username)
    global counter
    counter = 0
    start_date_timestamp = datetime.timestamp(datetime.strptime(start_date, '%Y-%m-%d %H:%M'))
    end_date_timestamp = datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d %H:%M'))
    posts = graph.get_connections(profile['id'],
                                  'posts?fields=message,created_time,reactions{profile_type,name,id,type},comments{id,message,from,reactions,like_count,created_time}',
                                  since=start_date_timestamp, until=end_date_timestamp)
    
    return posts


def filter_post_data(post, username, user_id, user_status, task_id, org_id, project_id, agency_id):
    post_data_dict = {}
    post_data_dict['is_updated'] = '1'
    post_data_dict['post_id'] = post.get('id')
    post_data_dict['post_message'] = post.get('message')
    post_data_dict['creation_time'] = post.get('created_time')
    if post.get('reactions'):
        post_data_dict['reactions_count'] = len(post['reactions']['data'])
    if post.get("comments"):
        post_data_dict['comments_count'] = len(post['comments']['data'])
    post_data_dict['username'] = username
    post_data_dict['user_id'] = user_id
    post_data_dict['user_status'] = user_status
    post_data_dict['task_id'] = task_id
    post_data_dict['org_id'] = org_id
    post_data_dict['project_id'] = project_id
    post_data_dict['agency_id'] = agency_id
    
    return post_data_dict


def post_table(post, db, user_id, user_status, username, counter, keyword_counter, keywords, message_count, u_id, p_id,
               s_id, task_id, project_id, org_id, agency_id):
    post_filtered_data = filter_post_data(post, username, user_id, user_status, task_id, org_id, project_id, agency_id)
    msg = post_filtered_data['post_message']
    is_filter_met = keyword_mapping(keywords, msg)
    if is_filter_met == 1:
        keyword_counter += 1
    if keyword_counter >= message_count:
        return {'2': 'Fetching Completed'}
    if counter >= 50 and keyword_counter == 0:
        return {'6': "Threshold Exceeded for Keyword Matching"}
    post_filtered_data['is_filter_met'] = is_filter_met
    
    db.posts.update_one({'post_id': post['id'],
                         'u_id': u_id,
                         'p_id': p_id,
                         's_id': s_id,
                         }, {'$set': post_filtered_data}, upsert=True)
    
    return post_filtered_data


def filter_comment_data(comment, username, user_id, user_status, task_id, project_id, org_id, agency_id):
    comment_data_dict = {}
    comment_data_dict['is_updated'] = '1'
    comment_data_dict['comment_id'] = comment.get('id')
    comment_data_dict['comment_message'] = comment.get('message')
    comment_data_dict['comment_like_count'] = comment.get('like_count')
    comment_data_dict['comment_created_time'] = comment.get('created_time')
    if comment.get('from'):
        comment_data_dict['commentor_name'] = comment['from']['name']
        comment_data_dict['commentor_id'] = comment['from']['id']
    if comment.get('reactions'):
        comment_data_dict['comment_reaction_count'] = len(comment['reactions']['data'])
    
    comment_data_dict['username'] = username
    comment_data_dict['user_id'] = user_id
    comment_data_dict['user_status'] = user_status
    comment_data_dict['task_id'] = task_id
    comment_data_dict['project_id'] = project_id
    comment_data_dict['org_id'] = org_id
    comment_data_dict['agency_id'] = agency_id
    
    return comment_data_dict


def comments_table(post, db, user_id, user_status, username, counter, keyword_counter, keywords, message_count, u_id,
                   p_id, s_id, task_id, project_id, org_id, agency_id):
    comment_data = []
    for comment in post.get('comments', {}).get('data', []):
        comment_filtered_data = filter_comment_data(comment, username, user_id, user_status, task_id, project_id,
                                                    org_id, agency_id)
        com_msg = comment_filtered_data['comment_message']
        is_filter_met = keyword_mapping(keywords, com_msg)
        if is_filter_met == 1:
            keyword_counter += 1
        if keyword_counter >= message_count:
            return {'2': 'Fetching Completed'}
        if counter >= 50 and keyword_counter == 0:
            return {'6': "Threshold Exceeded for Keyword Matching"}
        comment_filtered_data['post_id'] = post['id']
        comment_filtered_data['is_filter_met'] = is_filter_met
        
        db.comments.update_one({'comment_id': comment['id'],
                                'u_id': u_id,
                                'p_id': p_id,
                                's_id': s_id,
                                }, {'$set': comment_filtered_data}, upsert=True)
        comment_data.append(comment_filtered_data)
    return comment_data


def main_facebook(user_id, user_status, username, message_count, task_id, task_start_time, keywords, condition_set_id,
                  org_id, agency_id, project_id, start_date, end_date, credential_set, facebook_token):
    keyword_counter = 0
    graph = get_graph_api(facebook_token)
    posts = get_profile_posts(username, graph, start_date, end_date)
    db = get_mongodb_client("social", "Mango92Enj1", "test")
    message_count=int(message_count)
    error_message, ids = get_ids(username, user_id, project_id, agency_id, org_id, task_id)
    if error_message:
        return error_message
    (u_id, p_id, s_id) = ids
    
    post_data = [
        post_table(post, db, user_id, user_status, username, counter, keyword_counter, keywords, message_count, u_id,
                   p_id, s_id, task_id, project_id, org_id, agency_id) for post in posts['data']]
    comment_data = [
        comments_table(post, db, user_id, user_status, username, counter, keyword_counter, keywords, message_count,
                       u_id, p_id, s_id, task_id, project_id, org_id, agency_id) for post in posts['data']]
    
    return {"2": "Fetching Completed", "post_data": post_data, "comment_data": comment_data}


def get_stats(graph, start_date, end_date):
    profile = graph.get_object("me")
    start_date_timestamp = datetime.timestamp(datetime.strptime(start_date, '%Y-%m-%d %H:%M'))
    end_date_timestamp = datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d %H:%M'))
    stats = graph.get_all_connections(profile['id'],
                                      'insights?metric=page_impressions_unique,page_engaged_users,page_fans,page_fans_country,page_fans_city,page_fans_gender_age,page_fan_adds,page_fan_removes',
                                      since=start_date_timestamp, until=end_date_timestamp)
    return stats


def get_page_info(graph, start_date, end_date):
    profile = graph.get_object("me")
    start_date_timestamp = datetime.timestamp(datetime.strptime(start_date, '%Y-%m-%d %H:%M'))
    end_date_timestamp = datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d %H:%M'))
    info = graph.get_connections(profile['id'],
                                 '?fields=name,about,bio,category,location,display_subtext,engagement,features,link,new_like_count,overall_star_rating,rating_count,start_info,talking_about_count,username,website',
                                 since=start_date_timestamp, until=end_date_timestamp)
    return info


def filter_page_stats(stats, page_info, username, user_id, user_status, task_id, org_id, agency_id, project_id):
    stats_dict = page_info
    stats_dict['insights'] = stats.get('data')
    stats_dict['screen_name'] = username
    stats_dict['user_id'] = user_id
    stats_dict['task_id'] = task_id
    stats_dict['user_status'] = user_status
    stats_dict['org_id'] = org_id
    stats_dict['agency_id'] = agency_id
    stats_dict['project_id'] = project_id
    try:
        stats_dict['is_updated'] = '1'
        # stats_dict['name'] = page_info['name']
        # stats_dict['id'] = page_info['id']
        # stats_dict['about'] = page_info['about']
        # stats_dict['category'] = page_info['category']
        # stats_dict['username'] = page_info['username']
        # stats_dict['engagement'] = page_info['engagement']
        # stats_dict['location'] = page_info['location']
        # stats_dict['display_subtext'] = page_info['display_subtext']
        # stats_dict['link'] = page_info['link']
        # stats_dict['new_like_count'] = page_info['new_like_count']
        # stats_dict['start_info'] = page_info['start_info']
        # stats_dict['overall_star_rating'] = page_info['overall_star_rating']
        # stats_dict['rating_count'] = page_info['rating_count']
        # stats_dict['website'] = page_info.get('website')
    except:
        pass
    return stats_dict


def save_page_stats(db, stats_filtered, u_id, p_id, s_id):
    db.facebook_page_stats.update_one({'page_id': stats_filtered['id'],
                                      'u_id': str(u_id),
                                      'p_id': str(p_id),
                                      's_id': str(s_id),
                                      }, {'$set': stats_filtered}, upsert=True)


def stats_facebook(user_id, user_status, username, message_count, task_id, task_start_time, keywords, condition_set_id,
                   org_id, agency_id, project_id, start_date, end_date, credential_set, facebook_token):
    graph = get_graph_api(facebook_token)
    stats = get_stats(graph, start_date, end_date)
    page_info = get_page_info(graph, start_date, end_date)
    stats_filtered = filter_page_stats(stats, page_info, username, user_id, user_status, task_id, org_id, agency_id,
                                       project_id)
    db = get_mongodb_client("social", "Mango92Enj1", "test")
    error_message, ids = get_ids(username, user_id, project_id, agency_id, org_id, task_id)
    if error_message:
        return error_message
    (u_id, p_id, s_id) = ids
    save_page_stats(db, stats_filtered, u_id, p_id, s_id)
    
    return {"2": "Fetching Completed", 'data': stats_filtered}


def get_pages(graph):
    profile = graph.get_object("me")
    pages = graph.get_connections(profile['id'], 'accounts?fields=id,name,category,access_token')
    return pages.get("data")


def getPages_facebook(user_id, user_status, username, message_count, task_id, task_start_time, keywords,
                      condition_set_id, org_id, agency_id, project_id, start_date, end_date, credential_set,
                      facebook_token):
    graph = get_graph_api(facebook_token)
    pages = get_pages(graph)
    
    return {"data": pages}


# ALE


def get_long_lived_token(user_token):
    app_credential = pd.read_csv('../app_credentials.csv')
    client_id = app_credential['Client_id'].values[0]
    client_secret = app_credential['Client_secret'].values[0]
    graph = get_graph_api(user_token)
    token = graph.get_connections('oauth',
                                  f"access_token?grant_type=fb_exchange_token&client_id={client_id}&client_secret=" + client_secret + "&fb_exchange_token=" + user_token)
    
    access_token = token.get('access_token')
    
    return access_token


def facebook_authentication(user_token):
    user_long_token = get_long_lived_token(user_token)
    graph = get_graph_api(user_long_token)
    pages = get_pages(graph)
    for item in pages:
        item['user_token'] = user_long_token
    return {'user_token': user_long_token, 'data': pages}


def get_my_page_details(graph, page_id):
    info = graph.get_connections(page_id,
                                 '?fields=name,about,awards,band_interests,band_members,best_page,bio,birthday,'
                                 # 'business,'
                                 'category,company_overview,country_page_likes,description,display_subtext,'
                                 'emails,engagement,fan_count,featured_video,followers_count,founded,general_info,genre')
    return info


def get_public_page_details(graph, page_id):
    info = graph.get_connections(page_id,
                                 '?fields=name,link,about,bio,birthday,description,display_subtext,emails,fan_count,'
                                 'followers_count,general_info,hometown,location,new_like_count,personal_info,phone,'
                                 'username,website,features,founded,general_manager,genre,global_brand_page_name,'
                                 # 'has_added_app,'
                                 'has_whatsapp_business_number,has_whatsapp_number,is_always_open,'
                                 'is_chain,is_community_page,is_messenger_platform_bot,is_owned,is_permanently_closed')
    return info


def filter_my_page_details(page_info, user_status, user_id, task_id, condition_set_id, org_id, agency_id, project_id):
    page_dict = {}
    page_dict['name'] = page_info.get('name')
    page_dict['about'] = page_info.get('about')
    page_dict['page_id'] = page_info.get('id')
    page_dict['category'] = page_info.get('category')
    page_dict['description'] = page_info.get('description')
    page_dict['fan_count'] = page_info.get('fan_count')
    page_dict['followers_count'] = page_info.get('followers_count')
    page_dict['display_subtext'] = page_info.get('display_subtext')
    page_dict['emails'] = page_info.get('emails')
    page_dict['awards'] = page_info.get('awards')
    page_dict['birthday'] = page_info.get('birthday')
    page_dict['country_page_likes'] = page_info.get('country_page_likes')
    page_dict['engagement'] = page_info.get('engagement', {}).get('count')
    
    page_dict['user_status'] = user_status
    page_dict['user_id'] = user_id
    page_dict['task_id'] = task_id
    page_dict['condition_set_id'] = condition_set_id
    page_dict['org_id'] = org_id
    page_dict['project_id'] = project_id
    page_dict['agency_id'] = agency_id
    
    return page_dict


def save_my_page_detail(db, page_info_filtered, u_id, p_id, s_id):
    db.facebook_mypage_details.update_one({'page_id': page_info_filtered['page_id'],
                                           'u_id': str(u_id),
                                           'p_id': str(p_id),
                                           's_id': str(s_id),
                                           }, {'$set': page_info_filtered}, upsert=True)


def save_public_page_detail(db, page_info_filtered, u_id, p_id, s_id):
    db.facebook_page_details.update_one({'page_id': page_info_filtered['page_id'],
                                         'u_id': str(u_id),
                                         'p_id': str(p_id),
                                         's_id': str(s_id),
                                         }, {'$set': page_info_filtered}, upsert=True)


def get_posts(graph, page_id, start_date, end_date):
    start_date_timestamp = datetime.timestamp(datetime.strptime(start_date, '%Y-%m-%d %H:%M'))
    end_date_timestamp = datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d %H:%M'))
    posts = {'data': []}
    try:
        posts = graph.get_connections(page_id,
                                  'feed?fields=id,message,attachments,message_tags,created_time,permalink_url,shares,status_type,reactions{id,name,profile_type,type},comments{id,message,attachment,like_count,comment_count,from,created_time,reactions{id,name,profile_type,type},comments{id,message,attachment,like_count,comment_count,from,created_time,reactions{id,name,profile_type,type},comments}}',
                                  since=start_date_timestamp, until=end_date_timestamp)
    except Exception as e:
        print(e)
    return posts


def filter_my_post(post, graph, user_id, user_status, task_id, org_id, project_id, agency_id, condition_set_id):
    post_data_dict = {}
    post_data_dict['is_updated'] = '1'
    post_data_dict['post_id'] = post.get('id')
    post_data_dict['post_message'] = post.get('message')
    if post.get('message_tags'):
        post_data_dict['message_tags'] = post.get("message_tags")
    post_data_dict['created_time'] = post.get('created_time')
    post_data_dict['permalink_url'] = post.get('permalink_url')
    post_data_dict['status_type'] = post.get('status_type')
    if post.get('reactions'):
        post_data_dict['reactions_count'] = len(post['reactions']['data'])
        post_data_dict['reactor_list'] = [item.get('name') for item in post.get('reactions').get('data')]
    post_data_dict['reactions'] = post.get('reactions', {}).get('data')
    if post.get('comments'):
        post_data_dict['comments_count'] = len(post['comments']['data'])
    if post.get('attachments'):
        post_data_dict['attachments'] = post['attachments']['data']
    
    post_data_dict['shares'] = post.get('shares', {}).get('count', 0)
    
    post_data_dict['user_id'] = user_id
    post_data_dict['user_status'] = user_status
    post_data_dict['task_id'] = task_id
    post_data_dict['org_id'] = org_id
    post_data_dict['condition_set_id'] = condition_set_id
    post_data_dict['project_id'] = project_id
    post_data_dict['agency_id'] = agency_id
    
    return post_data_dict


def my_post_table(post, db, graph, user_id, user_status, username, keywords, message_count, u_id, p_id, s_id,
                  condition_set_id, task_id, project_id, org_id, agency_id, start_date, end_date, page_id):
    if keyword_mapping(keywords, post.get('message', '')) == 1 and post:
        post_filtered_data = filter_my_post(post, graph, user_id, user_status, task_id, org_id, project_id,
                                            agency_id, condition_set_id)
        media_df = pd.DataFrame.from_dict(post_filtered_data, orient='index')
        media_df = media_df.transpose()
        media_df['created_time'] = [parser.parse(x).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S") for x in
                                    media_df['created_time']]
        media_df = media_df[media_df['created_time'] >= parser.parse(start_date).strftime("%Y-%m-%d %H:%M:%S")]
        media_df = media_df[media_df['created_time'] <= parser.parse(end_date).strftime("%Y-%m-%d %H:%M:%S")]
        if media_df.empty:
            return None
        post_filtered_data = media_df.to_dict('records')[0]
        db.facebook_my_posts.update_one({'post_id': post['id'],
                                         'page_id': page_id,
                                         'u_id': u_id,
                                         'p_id': p_id,
                                         's_id': s_id,
                                         }, {'$set': post_filtered_data}, upsert=True)
    else:
        return None
    return post_filtered_data


def public_post_table(post, db, graph, user_id, user_status, username, keywords, message_count, u_id, p_id, s_id,
                      condition_set_id, task_id, project_id, org_id, agency_id, start_date, end_date, page_id):
    if keyword_mapping(keywords, post.get('message', '')) == 1 and post:
        post_filtered_data = filter_my_post(post, graph, user_id, user_status, task_id, org_id, project_id,
                                            agency_id, condition_set_id)
        media_df = pd.DataFrame.from_dict(post_filtered_data, orient='index')
        media_df = media_df.transpose()
        media_df['created_time'] = [parser.parse(x).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S") for x in
                                    media_df['created_time']]
        media_df = media_df[media_df['created_time'] >= parser.parse(start_date).strftime("%Y-%m-%d %H:%M:%S")]
        media_df = media_df[media_df['created_time'] <= parser.parse(end_date).strftime("%Y-%m-%d %H:%M:%S")]
        if media_df.empty:
            return None
        post_filtered_data = media_df.to_dict('records')[0]
        db.facebook_posts.update_one({'post_id': post['id'],
                                      'page_id': page_id,
                                      'u_id': u_id,
                                      'p_id': p_id,
                                      's_id': s_id,
                                      }, {'$set': post_filtered_data}, upsert=True)
    else:
        return None
    return post_filtered_data


def filter_my_comment_data(comment, post, user_id, user_status, task_id, project_id, org_id, agency_id,
                           condition_set_id):
    comment_data_dict = {}
    comment_data_dict['is_updated'] = '1'
    comment_data_dict['comment_id'] = comment.get('id')
    comment_data_dict['message'] = comment.get('message')
    comment_data_dict['like_count'] = comment.get('like_count')
    comment_data_dict['created_time'] = comment.get('created_time')
    comment_data_dict['commentor_name'] = comment.get('from', {}).get('name')
    comment_data_dict['commentor_id'] = comment.get('from', {}).get('id')
    if comment.get('reactions'):
        comment_data_dict['comment_reaction_count'] = len(comment['reactions']['data'])
        comment_data_dict['comment_reactions'] = comment.get('reactions').get('data')
    if comment.get('comments'):
        comment_data_dict['comment_replies'] = comment.get('comments').get('data')
        comment_data_dict['comment_reply_count'] = len(comment.get('comments', {}).get('data', []))
    if comment.get('attachment'):
        comment_data_dict['comment_attachment'] = comment.get('attachment').get('data')
    
    comment_data_dict['condition_set_id'] = condition_set_id
    comment_data_dict['user_id'] = user_id
    comment_data_dict['user_status'] = user_status
    comment_data_dict['task_id'] = task_id
    comment_data_dict['project_id'] = project_id
    comment_data_dict['org_id'] = org_id
    comment_data_dict['agency_id'] = agency_id
    
    return comment_data_dict


def my_post_comments_table(post, db, graph, user_id, user_status, username, keywords, message_count, u_id, p_id, s_id,
                           condition_set_id, task_id, project_id, org_id, agency_id, start_date, end_date, page_id):
    comment_data = []
    try:
        for comment in post['comments']['data']:
            try:
                if keyword_mapping(keywords, comment.get("message", '')) == 1:
                    comment_filtered_data = filter_my_comment_data(comment, post, user_id, user_status, task_id,
                                                                   project_id,
                                                                   org_id,
                                                                   agency_id, condition_set_id)
                    
                    comment_df = pd.DataFrame.from_dict(comment_filtered_data, orient='index')
                    comment_df = comment_df.transpose()
                    comment_df['created_time'] = [parser.parse(x).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S") for
                                                  x
                                                  in comment_df['created_time']]
                    comment_df = comment_df[
                        comment_df['created_time'] >= parser.parse(start_date).strftime("%Y-%m-%d %H:%M:%S")]
                    comment_df = comment_df[
                        comment_df['created_time'] <= parser.parse(end_date).strftime("%Y-%m-%d %H:%M:%S")]
                    if not comment_df.empty:
                        comment_filtered_data = comment_df.to_dict('records')[0]
                        
                        db.facebook_myposts_comments.update_one({'comment_id': comment['id'],
                                                                 'page_id': page_id,
                                                                 'post_id': post["id"],
                                                                 'u_id': u_id,
                                                                 'p_id': p_id,
                                                                 's_id': s_id,
                                                                 }, {'$set': comment_filtered_data}, upsert=True)
                        comment_data.append(comment_filtered_data)
                else:
                    pass
            except Exception as error:
                print(error)
                pass
    except:
        pass
    if comment_data:
        return comment_data


def public_post_comments_table(post, db, graph, user_id, user_status, username, keywords, message_count, u_id, p_id,
                               s_id,
                               condition_set_id, task_id, project_id, org_id, agency_id, start_date, end_date, page_id):
    comment_data = []
    try:
        for comment in post['comments']['data']:
            try:
                if keyword_mapping(keywords, comment.get("message", '')) == 1 and comment:
                    comment_filtered_data = filter_my_comment_data(comment, post, user_id, user_status, task_id,
                                                                   project_id,
                                                                   org_id,
                                                                   agency_id, condition_set_id)
                    
                    comment_df = pd.DataFrame.from_dict(comment_filtered_data, orient='index')
                    comment_df = comment_df.transpose()
                    comment_df['created_time'] = [parser.parse(x).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S") for
                                                  x in comment_df['created_time']]
                    comment_df = comment_df[
                        comment_df['created_time'] >= parser.parse(start_date).strftime("%Y-%m-%d %H:%M:%S")]
                    comment_df = comment_df[
                        comment_df['created_time'] <= parser.parse(end_date).strftime("%Y-%m-%d %H:%M:%S")]
                    if not comment_df.empty:
                        comment_filtered_data = comment_df.to_dict('records')[0]
                        
                        db.facebook_posts_comments.update_one({'comment_id': comment['id'],
                                                               'page_id': page_id,
                                                               'post_id': post["id"],
                                                               'u_id': u_id,
                                                               'p_id': p_id,
                                                               's_id': s_id,
                                                               }, {'$set': comment_filtered_data}, upsert=True)
                        comment_data.append(comment_filtered_data)
                else:
                    pass
            except Exception as error:
                print(error)
                pass
    except:
        pass
    if comment_data:
        return comment_data


def get_tagged_data(graph, page_id, start_date, end_date):
    start_date_timestamp = datetime.timestamp(datetime.strptime(start_date, '%Y-%m-%d %H:%M'))
    end_date_timestamp = datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d %H:%M'))
    tag_data = graph.get_connections(page_id,
                                     'tagged?fields=message,tagged_time,from,instagram_eligibility,reactions,comments',
                                     since=start_date_timestamp, until=end_date_timestamp)
    
    return tag_data


def filter_tagged_data(post, graph, user_id, user_status, task_id, org_id, project_id, agency_id, condition_set_id):
    post_data_dict = {}
    post_data_dict['is_updated'] = '1'
    post_data_dict['tagged_post_id'] = post.get('id')
    post_data_dict['message'] = post.get('message')
    post_data_dict['instagram_eligibility'] = post.get('message_tags')
    post_data_dict['tagged_time'] = post.get('tagged_time')
    if post.get('reactions'):
        post_data_dict['reactions_count'] = len(post['reactions']['data'])
        post_data_dict['reactor_list'] = [item.get('name') for item in post.get('reaction').get('data')]
    post_data_dict['reactions'] = post.get('reactions', {}).get('data')
    
    if post.get('comments'):
        post_data_dict['comments_count'] = len(post['comments']['data'])
    post_data_dict['comments_count'] = post.get('comments', {}).get('data')
    
    post_data_dict['user_id'] = user_id
    post_data_dict['user_status'] = user_status
    post_data_dict['task_id'] = task_id
    post_data_dict['org_id'] = org_id
    post_data_dict['condition_set_id'] = condition_set_id
    post_data_dict['project_id'] = project_id
    post_data_dict['agency_id'] = agency_id
    
    return post_data_dict


def tagged_table(post, db, graph, user_id, user_status, username, keywords, message_count, u_id, p_id, s_id,
                 condition_set_id, task_id, project_id, org_id, agency_id, start_date, end_date):
    if keyword_mapping(keywords, post.get('message', '')) == 1 and post:
        post_filtered_data = filter_tagged_data(post, graph, user_id, user_status, task_id, org_id, project_id,
                                                agency_id, condition_set_id)
        media_df = pd.DataFrame.from_dict(post_filtered_data, orient='index')
        media_df = media_df.transpose()
        media_df['tagged_time'] = [parser.parse(x).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S") for x in
                                   media_df['tagged_time']]
        media_df = media_df[media_df['tagged_time'] >= parser.parse(start_date).strftime("%Y-%m-%d %H:%M:%S")]
        media_df = media_df[media_df['tagged_time'] <= parser.parse(end_date).strftime("%Y-%m-%d %H:%M:%S")]
        if media_df.empty:
            return None
        post_filtered_data = media_df.to_dict('records')[0]
        db.facebook_tagged_posts.update_one({'tagged_post_id': post.get('id'),
                                             'u_id': u_id,
                                             'p_id': p_id,
                                             's_id': s_id,
                                             }, {'$set': post_filtered_data}, upsert=True)
    else:
        return None
    return post_filtered_data


def my_page(user_id, user_status, username, message_count, task_id, task_start_time, keywords, condition_set_id, org_id,
            agency_id, project_id, start_date, end_date, user_token, page_token, page_id):
    graph = get_graph_api(page_token)
    error_message, ids = get_ids(username, user_id, project_id, agency_id, org_id, task_id)
    if error_message:
        return error_message
    (u_id, p_id, s_id) = ids
    db = get_mongodb_client("social", "Mango92Enj1", "test")
    page_info = get_my_page_details(graph, page_id)
    page_info_filtered = filter_my_page_details(page_info, user_status, user_id, task_id, condition_set_id, org_id,
                                                agency_id, project_id)
    page_info_filtered["screen_name"] = username
    page_info_filtered["page_id"] = page_id
    page_info_filtered["insights"] = get_page_stats(start_date, end_date, page_token, page_id)
    save_my_page_detail(db, page_info_filtered, u_id, p_id, s_id)
    
    posts = get_posts(graph, page_id, start_date, end_date)
    
    post_data = list(filter(None, [
        my_post_table(post, db, graph, user_id, user_status, username, keywords, message_count, u_id, p_id, s_id,
                      condition_set_id, task_id, project_id, org_id, agency_id, start_date, end_date, page_id) for post in
        posts.get('data')]))
    comment_data = list(filter(None, [
        my_post_comments_table(post, db, graph, user_id, user_status, username, keywords, message_count, u_id, p_id,
                               s_id, condition_set_id, task_id, project_id, org_id, agency_id, start_date, end_date, page_id) for
        post in posts.get('data') if 'comments' in post]))
    
    tagged = get_tagged_data(graph, page_id, start_date, end_date)
    tagged_data = list(filter(None, [
        tagged_table(tag_post, db, graph, user_id, user_status, username, keywords, message_count, u_id, p_id, s_id,
                     condition_set_id, task_id, project_id, org_id, agency_id, start_date, end_date) for tag_post in
        tagged.get('data')]))
    
    return {'2': 'Fetching Completed', 'posts': len(post_data), 'comments': len(comment_data), 'tagged': len(tagged_data)}


def public_page(user_id, user_status, username, message_count, task_id, task_start_time, keywords, condition_set_id,
                org_id,
                agency_id, project_id, start_date, end_date, user_token, page_token):
    graph = get_graph_api()
    graph_user = get_graph_api(user_token)
    error_message, ids = get_ids(username, user_id, project_id, agency_id, org_id, task_id)
    if error_message:
        return error_message
    (u_id, p_id, s_id) = ids
    db = get_mongodb_client("social", "Mango92Enj1", "test")
    
    page_info = get_public_page_details(graph, username)
    page_id = page_info.get('id')
    page_info_filtered = filter_my_page_details(page_info, user_status, user_id, task_id, condition_set_id, org_id,
                                                agency_id, project_id)
    page_info_filtered["screen_name"] = username
    save_public_page_detail(db, page_info_filtered, u_id, p_id, s_id)
    
    posts = get_posts(graph_user, page_id, start_date, end_date)
    
    post_data = list(filter(None, [
        public_post_table(post, db, graph, user_id, user_status, username, keywords, message_count, u_id, p_id, s_id,
                          condition_set_id, task_id, project_id, org_id, agency_id, start_date, end_date, page_id) for post in
        posts.get('data', [])]))
    
    comment_data = list(filter(None, [
        public_post_comments_table(post, db, graph, user_id, user_status, username, keywords, message_count, u_id, p_id,
                                   s_id, condition_set_id, task_id, project_id, org_id, agency_id, start_date, end_date, page_id)
        for
        post in posts.get('data') if 'comments' in post]))
    
    return {'2': 'Fetching Completed', 'posts': len(post_data), 'comments': len(comment_data)}


# Action

def get_my_insights(graph, page_id, start_date, end_date):
    start_date_timestamp = datetime.timestamp(datetime.strptime(start_date, '%Y-%m-%d %H:%M'))
    end_date_timestamp = datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d %H:%M'))
    
    day = graph.get_connections(page_id,
                                'insights?metric=page_fans_city,page_fans_country,page_impressions,page_fans_online,page_fans_gender_age,page_views_total,page_engaged_users,page_post_engagements,page_posts_impressions,post_engaged_users,post_engaged_fan,post_clicks,page_actions_post_reactions_total&period=day',
                                since=start_date_timestamp, until=end_date_timestamp)
    
    week = graph.get_connections(page_id,
                                 'insights?metric=page_impressions,page_views_total,page_engaged_users,page_post_engagements,page_posts_impressions,post_engaged_users,post_engaged_fan,post_clicks,page_actions_post_reactions_total&period=week',
                                 since=start_date_timestamp, until=end_date_timestamp)
    
    month = graph.get_connections(page_id,
                                  'insights?metric=page_impressions,page_views_total,page_engaged_users,page_post_engagements,page_posts_impressions,post_engaged_users,post_engaged_fan,post_clicks,page_actions_post_reactions_total&period=days_28',
                                  since=start_date, until=end_date_timestamp)
    lifetime = graph.get_connections(page_id,
                                     'insights?metric=page_impressions,page_views_total,page_engaged_users,page_post_engagements,page_posts_impressions,post_engaged_users,post_engaged_fan,post_clicks,page_actions_post_reactions_total&period=days_28')
    return day, week, month, lifetime


def get_page_stats(start_date, end_date, page_token, page_id):
    graph = get_graph_api(page_token)
    day, week, month, lifetime = get_my_insights(graph, page_id, start_date, end_date)
    return {'day': day, 'week': week, 'month': month, 'lifetime': lifetime}


def get_post_insights(graph, post_id, start_date, end_date):
    start_date_timestamp = datetime.timestamp(datetime.strptime(start_date, '%Y-%m-%d %H:%M'))
    end_date_timestamp = datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d %H:%M'))
    
    post_insight = graph.get_connections(post_id,
                                         'insights/post_reactions_by_type_total,post_impressions,post_engaged_users,post_engaged_fan,post_clicks',
                                         since=start_date_timestamp, until=end_date_timestamp)
    return post_insight


def filter_post_stats(post, graph, start_date, end_date):
    post_data_dict = {}
    post_data_dict['post_id'] = post.get('id')
    post_data_dict['post_message'] = post.get('message')
    if post.get('message_tags'):
        post_data_dict['message_tags'] = post.get("message_tags")
    post_data_dict['created_time'] = post.get('created_time')
    post_data_dict['permalink_url'] = post.get('permalink_url')
    if post.get('reactions'):
        post_data_dict['reactions_count'] = len(post['reactions']['data'])
    else:
        post_data_dict['reactions_count'] = 0
    if post.get('comments'):
        post_data_dict['comments_count'] = len(post['comments']['data'])
    else:
        post_data_dict['comments_count'] = 0
    
    post_data_dict['shares'] = post.get('shares', {}).get('count', 0)
    
    post_data_dict['insights'] = get_post_insights(graph, post.get('id'), start_date, end_date)
    
    return post_data_dict


def get_post_stats(start_date, end_date, page_token, page_id):
    graph = get_graph_api(page_token)
    posts = get_posts(graph, page_id, start_date, end_date)
    post_stats = [filter_post_stats(post, graph, start_date, end_date) for post in posts.get('data')]
    return {'post_stats': post_stats}


def get_videos(graph, page_id, start_date, end_date):
    start_date_timestamp = datetime.timestamp(datetime.strptime(start_date, '%Y-%m-%d %H:%M'))
    end_date_timestamp = datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d %H:%M'))
    videos = graph.get_connections(page_id,
                                   'videos?fields=backdated_time,backdated_time_granularity,content_category,content_tags,ad_breaks,created_time,custom_labels,description,embed_html,embeddable,event,format,from,icon,id,is_crosspost_video,is_crossposting_eligible,is_episode,is_instagram_eligible,length,live_status,picture,place,premiere_living_room_status,privacy,published,scheduled_publish_time,source,status,title,universal_video_id,updated_time,captions,comments,thumbnails,crosspost_shared_pages,likes,permalink_url,poll_settings,polls,sponsor_tags,tags,video_insights',
                                   since=start_date_timestamp, until=end_date_timestamp)
    return videos


def get_video_insights(graph, video_id, start_date, end_date):
    start_date_timestamp = datetime.timestamp(datetime.strptime(start_date, '%Y-%m-%d %H:%M'))
    end_date_timestamp = datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d %H:%M'))
    video_insights = graph.get_connections(video_id,
                                           'video_insights?metric=total_video_views, total_video_views_unique, total_video_views_autoplayed, total_video_views_clicked_to_play, total_video_views_organic, total_video_views_organic_unique, total_video_views_paid, total_video_views_paid_unique, total_video_views_sound_on, total_video_complete_views, total_video_complete_views_unique, total_video_complete_views_auto_played, total_video_complete_views_clicked_to_play, total_video_complete_views_organic, total_video_complete_views_organic_unique, total_video_complete_views_paid,total_video_complete_views_paid_unique,total_video_10s_views,total_video_10s_views_unique,total_video_10s_views_auto_played,total_video_10s_views_clicked_to_play,total_video_10s_views_organic,total_video_10s_views_paid,total_video_10s_views_sound_on,total_video_15s_views,total_video_60s_excludes_shorter_views,total_video_retention_graph,total_video_retention_graph_autoplayed,total_video_retention_graph_clicked_to_play,total_video_avg_time_watched,total_video_view_total_time,total_video_view_total_time_organic,total_video_view_total_time_paid,total_video_impressions,total_video_impressions_unique,total_video_impressions_paid_unique,total_video_impressions_paid,total_video_impressions_organic_unique,total_video_impressions_organic,total_video_impressions_viral_unique,total_video_impressions_viral,total_video_impressions_fan_unique,total_video_impressions_fan,total_video_impressions_fan_paid_unique,total_video_impressions_fan_paid,total_video_stories_by_action_type,total_video_reactions_by_type_total,total_video_view_time_by_age_bucket_and_gender,total_video_view_time_by_region_id,total_video_views_by_distribution_type,total_video_view_time_by_distribution_type',
                                           since=start_date_timestamp, until=end_date_timestamp)
    return video_insights


def filter_video_stats(video, graph, start_date, end_date):
    video['video_insights'] = get_video_insights(graph, video.get('id'), start_date, end_date)
    return video


def get_my_videos(start_date, end_date, page_token, page_id):
    graph = get_graph_api(page_token)
    videos = get_videos(graph, page_id, start_date, end_date)
    video_details = [filter_video_stats(video, graph, start_date, end_date) for video in videos.get('data')]
    return {'video_deatils': video_details}
