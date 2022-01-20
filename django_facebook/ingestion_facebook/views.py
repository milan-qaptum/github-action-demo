import sys
from django.shortcuts import render
from rest_framework.decorators import api_view
from rest_framework.response import Response

sys.path.append('../Code/')
from facebook_ingest import check_username as fcu
from facebook_ingest import main_facebook, stats_facebook, getPages_facebook, facebook_authentication, my_page, \
    search_page, public_page,get_page_stats,get_post_stats,get_my_videos


# from .tasks import celery_task
# from .tasks import celery_checkuser

@api_view(['GET', 'POST'])
def facebook_function(request):
    if request.method == 'GET':
        return Response({"message": "Got some data!"})
    
    elif request.method == 'POST':
        print(request.data)
        user_id = request.data['user_id']
        user_status = request.data['user_status']
        username = request.data['username']
        message_count = request.data['message_count']
        task_id = request.data['task_id']
        task_start_time = request.data['task_start_time']
        keywords = request.data['keywords']
        condition_set_id = request.data['condition_set_id']
        org_id = request.data['org_id']
        agency_id = request.data['agency_id']
        project_id = request.data['project_id']
        start_date = request.data['start_date']
        end_date = request.data['end_date']
        credential_set = request.data['credential_set']
        facebook_token = request.data['facebook_token']
        print(user_id, user_status, username, message_count, task_id, task_start_time, keywords, condition_set_id,
              org_id, agency_id, project_id, start_date, end_date)
        result = main_facebook(user_id, user_status, username, message_count, task_id, task_start_time, keywords,
                               condition_set_id, org_id, agency_id, project_id, start_date, end_date, credential_set,
                               facebook_token)
        # print(user_id,user_status,start_datetime,stop_datetime,username)
        # result = celery_task.delay(user_id, user_status, start_datetime, stop_datetime, username)
        # result = main_facebook(user_id, user_status, start_datetime, stop_datetime, username)
        return Response({"message": result})


@api_view(['GET', 'POST'])
def facebook_checkUser(request):
    if request.method == 'GET':
        return Response({"message": "Got some data!"})
    
    elif request.method == 'POST':
        print(request.data)
        username = request.data['username']
        user_status = request.data['user_status']
        result = fcu(username=username, user_status=user_status)
        return Response({"message": result})


# result = fcu(username=username)
# result = celery_checkuser.delay(username=username)
# return Response({"message": result})

@api_view(['GET', 'POST'])
def facebook_stats(request):
    if request.method == 'GET':
        return Response({"message": "Got some data!"})
    
    elif request.method == 'POST':
        print(request.data)
        user_id = request.data['user_id']
        user_status = request.data['user_status']
        username = request.data['username']
        message_count = request.data['message_count']
        task_id = request.data['task_id']
        task_start_time = request.data['task_start_time']
        keywords = request.data['keywords']
        condition_set_id = request.data['condition_set_id']
        org_id = request.data['org_id']
        agency_id = request.data['agency_id']
        project_id = request.data['project_id']
        start_date = request.data['start_date']
        end_date = request.data['end_date']
        credential_set = request.data['credential_set']
        facebook_token = request.data['facebook_token']
        print(user_id, user_status, username, message_count, task_id, task_start_time, keywords, condition_set_id,
              org_id, agency_id, project_id, start_date, end_date)
        result = stats_facebook(user_id, user_status, username, message_count, task_id, task_start_time, keywords,
                                condition_set_id, org_id, agency_id, project_id, start_date, end_date, credential_set,
                                facebook_token)
        # print(user_id,user_status,start_datetime,stop_datetime,username)
        # result = celery_task.delay(user_id, user_status, start_datetime, stop_datetime, username)
        # result = main_facebook(user_id, user_status, start_datetime, stop_datetime, username)
        return Response({"message": result})


@api_view(['GET', 'POST'])
def facebook_getPages(request):
    if request.method == 'GET':
        return Response({"message": "Got some data!"})
    
    elif request.method == 'POST':
        print(request.data)
        user_id = request.data['user_id']
        user_status = request.data['user_status']
        username = request.data['username']
        message_count = request.data['message_count']
        task_id = request.data['task_id']
        task_start_time = request.data['task_start_time']
        keywords = request.data['keywords']
        condition_set_id = request.data['condition_set_id']
        org_id = request.data['org_id']
        agency_id = request.data['agency_id']
        project_id = request.data['project_id']
        start_date = request.data['start_date']
        end_date = request.data['end_date']
        credential_set = request.data['credential_set']
        facebook_token = request.data['facebook_token']
        print(user_id, user_status, username, message_count, task_id, task_start_time, keywords, condition_set_id,
              org_id, agency_id, project_id, start_date, end_date)
        result = getPages_facebook(user_id, user_status, username, message_count, task_id, task_start_time, keywords,
                                   condition_set_id, org_id, agency_id, project_id, start_date, end_date,
                                   credential_set, facebook_token)
        # print(user_id,user_status,start_datetime,stop_datetime,username)
        # result = celery_task.delay(user_id, user_status, start_datetime, stop_datetime, username)
        # result = main_facebook(user_id, user_status, start_datetime, stop_datetime, username)
        return Response({"message": result})


@api_view(['GET', 'POST'])
def facebook_auth(request):
    if request.method == 'GET':
        return Response({"message": "Got some data!"})
    elif request.method == 'POST':
        print(request.data)
        user_token = request.data['user_token']
        result = facebook_authentication(user_token)
        return Response({"message": result})


@api_view(['GET', 'POST'])
def search(request):
    if request.method == 'GET':
        return Response({"message": "Got some data!"})
    
    elif request.method == 'POST':
        print(request.data)
        query = request.data['query']
        result = search_page(query)
        return Response({"message": result})


@api_view(['GET', 'POST'])
def public_Page(request):
    if request.method == 'GET':
        return Response({"message": "Got some data!"})
    
    elif request.method == 'POST':
        print(request.data)
        user_id = request.data['user_id']
        user_status = request.data['user_status']
        username = request.data['page_name']
        message_count = request.data['message_count']
        task_id = request.data['task_id']
        task_start_time = request.data['task_start_time']
        keywords = request.data['keywords']
        condition_set_id = request.data['condition_set_id']
        org_id = request.data['org_id']
        agency_id = request.data['agency_id']
        project_id = request.data['project_id']
        start_date = request.data['start_date']
        end_date = request.data['end_date']
        user_token = request.data['user_token']
        page_token = request.data.get('page_token')
        # page_id = request.data['page_id']
        result = public_page(user_id, user_status, username, message_count, task_id, task_start_time, keywords,
                         condition_set_id, org_id, agency_id, project_id, start_date, end_date, user_token, page_token)
        # print(user_id,user_status,start_datetime,stop_datetime,username)
        # result = celery_task.delay(user_id, user_status, start_datetime, stop_datetime, username)
        # result = main_facebook(user_id, user_status, start_datetime, stop_datetime, username)
        return Response({"message": result})


@api_view(['GET', 'POST'])
def my_Page(request):
    if request.method == 'GET':
        return Response({"message": "Got some data!"})
    
    elif request.method == 'POST':
        print(request.data)
        user_id = request.data['user_id']
        user_status = request.data['user_status']
        username = request.data['page_name']
        message_count = request.data['message_count']
        task_id = request.data['task_id']
        task_start_time = request.data['task_start_time']
        keywords = request.data['keywords']
        condition_set_id = request.data['condition_set_id']
        org_id = request.data['org_id']
        agency_id = request.data['agency_id']
        project_id = request.data['project_id']
        start_date = request.data['start_date']
        end_date = request.data['end_date']
        user_token = request.data['user_token']
        page_token = request.data['page_token']
        page_id = request.data['page_id']
        result = my_page(user_id, user_status, username, message_count, task_id, task_start_time, keywords,
                         condition_set_id, org_id, agency_id, project_id, start_date, end_date, user_token, page_token,page_id)
        # print(user_id,user_status,start_datetime,stop_datetime,username)
        # result = celery_task.delay(user_id, user_status, start_datetime, stop_datetime, username)
        # result = main_facebook(user_id, user_status, start_datetime, stop_datetime, username)
        return Response({"message": result})


@api_view(['GET', 'POST'])
def get_Pagestats(request):
    if request.method == 'GET':
        return Response({"message": "Got some data!"})
    
    elif request.method == 'POST':
        print(request.data)
        start_date = request.data['start_date']
        end_date = request.data['end_date']
        page_token = request.data['page_token']
        page_id = request.data['page_id']
        result = get_page_stats(start_date, end_date, page_token,page_id)
        # print(user_id,user_status,start_datetime,stop_datetime,username)
        # result = celery_task.delay(user_id, user_status, start_datetime, stop_datetime, username)
        # result = main_facebook(user_id, user_status, start_datetime, stop_datetime, username)
        return Response({"message": result})

@api_view(['GET', 'POST'])
def get_Poststats(request):
    if request.method == 'GET':
        return Response({"message": "Got some data!"})
    
    elif request.method == 'POST':
        print(request.data)
        start_date = request.data['start_date']
        end_date = request.data['end_date']
        page_token = request.data['page_token']
        page_id = request.data['page_id']
        result = get_post_stats(start_date, end_date, page_token,page_id)
        # print(user_id,user_status,start_datetime,stop_datetime,username)
        # result = celery_task.delay(user_id, user_status, start_datetime, stop_datetime, username)
        # result = main_facebook(user_id, user_status, start_datetime, stop_datetime, username)
        return Response({"message": result})

@api_view(['GET', 'POST'])
def get_Poststats(request):
    if request.method == 'GET':
        return Response({"message": "Got some data!"})
    
    elif request.method == 'POST':
        print(request.data)
        start_date = request.data['start_date']
        end_date = request.data['end_date']
        page_token = request.data['page_token']
        page_id = request.data['page_id']
        result = get_post_stats(start_date, end_date, page_token,page_id)
        # print(user_id,user_status,start_datetime,stop_datetime,username)
        # result = celery_task.delay(user_id, user_status, start_datetime, stop_datetime, username)
        # result = main_facebook(user_id, user_status, start_datetime, stop_datetime, username)
        return Response({"message": result})

@api_view(['GET', 'POST'])
def get_Myvideos(request):
    if request.method == 'GET':
        return Response({"message": "Got some data!"})
    
    elif request.method == 'POST':
        print(request.data)
        start_date = request.data['start_date']
        end_date = request.data['end_date']
        page_token = request.data['page_token']
        page_id = request.data['page_id']
        result = get_my_videos(start_date, end_date, page_token,page_id)
        # print(user_id,user_status,start_datetime,stop_datetime,username)
        # result = celery_task.delay(user_id, user_status, start_datetime, stop_datetime, username)
        # result = main_facebook(user_id, user_status, start_datetime, stop_datetime, username)
        return Response({"message": result})
