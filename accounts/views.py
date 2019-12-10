from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import User, Group
from django.contrib import auth, messages
from django.db import transaction
from django.shortcuts import render, redirect, get_object_or_404

from accounts.forms import UserModelForm, ProfileModelForm

@login_required
@transaction.atomic
def update_profile(request):
    print('request.method',request.method)
    context = {}
    if request.method == 'POST':
        user_form = UserModelForm(request.POST, instance=request.user)
        profile_form = ProfileModelForm(request.POST, instance=request.user.profile)
        if user_form.is_valid() and profile_form.is_valid():
            user_form.save()
            profile_form.save()
            messages.success(request, ('Your profile was successfully updated!'))
            return redirect('profile')
        else:
            print('error, cleaned_data',user_form,profile_form)
            messages.error(request, ('Please correct the error below.'))
    else:
        user_form = UserModelForm(instance=request.user)
        profile_form = ProfileModelForm(instance=request.user.profile)
        id_ = request.user.id
        u = get_object_or_404(User, id=id_)
        context['groups'] = u.groups.values_list('name',flat=True)
        
    return render(request, 'accounts/profile.html', {
        'user_form': user_form,
        'profile_form': profile_form,
        'context': context
    })
    
def register(request):
    if request.method == 'POST':
        if request.POST['password1'] == request.POST['password2']:
            try:
                User.objects.get(username=request.POST['username'])
                return render(request, 'accounts/register.html', {'error': 'User ID is already taken'})
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
                user.profile.user_type=request.POST['user_type']
                user.profile.name=request.POST['name']
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

    #class UserProfileView(UpdateView):
        #form_class = (UserModelForm,ProfileModelForm)
        #template_name = 'accounts/profile.html'
    
        ##print('request from profile',request.META.keys())
        
        #def get_success_url(self):
            #id_ = self.kwargs.get("id")
            #return '/accounts/profile'
        
        #def form_valid(self, form):
            #context={}
            #if all((user_form.is_valid(), profile_form.is_valid())):
            ##if form.is_valid():
                #print('forms are valid')
                #print('cleaned_data: before ->', form.cleaned_data)
            #else:
                #print('form not valid', form.errors)
                #context['errors'] = form.errors
            #return super().form_valid(form)
        
        #def get_object(self):
            #me = self.request.user
            ##print('args, kwargs:',self.args, self.kwargs)
            #id_ = me.id
            #return get_object_or_404(User, id=id_)
        
        #def get_context_data(self, *args, **kwargs):
            #id_ = self.request.user.id
            #u = get_object_or_404(User, id=id_)
            #context = super(UserProfileView, self).get_context_data(*args, **kwargs)
            #context['groups'] = u.groups.values_list('name',flat=True)
            #return context
            
        
        ##return render(request, 'accounts/profile.html', context=context)
    
    

