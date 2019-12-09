from django.urls import path, include
from . import views

urlpatterns = [
    path('register/', views.register, name='register'),
    path('login/', views.login, name='login'),
    path('logout/', views.logout, name='logout'),
    #path('<int:id>/detail', views.UserProfileView.as_view(), name='user-profile'),
    path('profile/', views.UserProfileView.as_view(), name='user-profile'),
    #path('profile/', views.profile, name='profile'),
]
