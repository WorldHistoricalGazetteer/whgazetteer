from django.shortcuts import render, redirect
from django.contrib.auth.models import User, Group
from django.contrib import auth

def profile(request):
    context={}
    context['groups'] = request.user.groups.values_list('name',flat=True)
    print('request from profile',request.META.keys())
    return render(request, 'accounts/profile.html', context=context)

def register(request):
    if request.method == 'POST':
        if request.POST['password1'] == request.POST['password2']:
            try:
                User.objects.get(username=request.POST['username'])
                return render(request, 'accounts/register.html', {'error': 'User name is already taken'})
            except User.DoesNotExist:
                print('request.POST',request.POST)
                user = User.objects.create_user(
                    request.POST['username'], 
                    password=request.POST['password1'],
                    email=request.POST['email'],
                    first_name=request.POST['first_name'],
                    last_name=request.POST['last_name']
                )
                user.profile.affiliation=request.POST['affiliation']
                auth.login(request, user)
                return redirect('home')
        else:
            return render(request, 'accounts/register.html', {'error': 'Sorry, password mismatch!'})
    else:
        return render(request, 'accounts/register.html')

def login(request):
    if request.method == 'POST':
        user = auth.authenticate(username=request.POST['username'],password=request.POST['password'])
        if user is not None:
            auth.login(request,user)
            return redirect('dashboard')
        else:
            return redirect('home', {'error': 'username or password is incorrect :^('})
    else:
        return render(request, 'accounts/login.html')

def logout(request):
    if request.method == 'POST':
        auth.logout(request)
        return redirect('home')
