from django.shortcuts import render


def maps_home(request):
    return render(request, 'maps/maps_home.html')


# not in use
def map_view(request, mid):
    context = {'mboxid': 'mapbox://styles/kgeographer/'+mid}
    return render(request, 'maps/map_view.html', context = context)
