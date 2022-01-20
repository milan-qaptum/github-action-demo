from django.urls import path, include
from . import views

urlpatterns = [
    path('facebook/', views.facebook_function),
    path('facebook/checkuser/', views.facebook_checkUser),
    path('facebook/stats/', views.facebook_stats),
    path('facebook/getpages/', views.facebook_getPages),
    
    # ALE
    path('facebook/auth/', views.facebook_auth),
    path('facebook/checkpage/', views.facebook_checkUser),
    path('facebook/searchpage/', views.search),
    path('facebook/publicpageinfo/', views.public_Page),
    path('facebook/mypageinfo/', views.my_Page),

    # Action
    path('facebook/action/getpagestats/', views.get_Pagestats),
    path('facebook/action/getpoststats/', views.get_Poststats),
    path('facebook/action/getmyvideos/', views.get_Myvideos),
]
