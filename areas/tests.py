from django.test import TestCase

# Create your tests here.
from django.contrib.auth.models import User
from django.shortcuts import get_object_or_404
from .models import Area
from areas.forms import AreaModelForm

class AreaFormTestCase(TestCase):
    def test_valid_form(self):
        User.objects.create(username='Satch')
        owner = get_object_or_404(User, id=1)
        id=1
        type = 'ccodes'
        title = 'Latvia hull'
        description = 'how now?'
        ccodes = ['lt']
        geojson = {
            "type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[21.07439212300011,55.74494049700003],[21.04468834700006,55.87372467700014],[21.053398015984072,56.07260387399778],[21.229639119000097,56.16318796800009],[21.59602502500013,56.3078819790001],[22.094082479000065,56.417410177000065],[24.89225793400007,56.43872670600014],[25.64967940300005,56.1438093070001],[26.594531291000067,55.66699086600015],[26.768629191000088,55.3002432250001],[25.763057495000055,54.15643707300002],[25.740009806000103,54.14625681600016],[24.37787072800009,53.88684112600008],[23.64261844900011,53.898985087000156],[23.485625448000064,53.939292704000124],[22.76721968600006,54.35626983700003],[21.267602287364923,55.24867008630527],[21.07439212300011,55.74494049700003]]]}}
        obj = Area.objects.create(
            owner=owner,type=type,title=title,description=description,ccodes=ccodes,geojson=geojson)
        data = {"owner":owner.id,"id":id,"type":type,"title":title,"description":description,
            "ccodes":ccodes,"geojson":geojson}
        form = AreaModelForm(data=data)
        # self.assertFormError(form, geom, 'AssertionError', 'fubar, fucking ridic')
        self.assertTrue(form.is_valid())
        self.assertEqual(form.cleaned_data.get('geojson'), obj.geojson)

# class AreaModelTestCase(TestCase):
#     def setUp(self):
#         User.objects.create(username='Satch')
#         Area.objects.create(
#             owner=get_object_or_404(User,id=2),
#             type='ccodes',
#             title='Testy',
#             description='how now?',
#             id=1,
#             ccodes=['ar'],
#             geom={"type":"Feature","properties":{},"geometry":{"type":"Polygon","coordinates":[[[21.07439212300011,55.74494049700003],[21.04468834700006,55.87372467700014],[21.053398015984072,56.07260387399778],[21.229639119000097,56.16318796800009],[21.59602502500013,56.3078819790001],[22.094082479000065,56.417410177000065],[24.89225793400007,56.43872670600014],[25.64967940300005,56.1438093070001],[26.594531291000067,55.66699086600015],[26.768629191000088,55.3002432250001],[25.763057495000055,54.15643707300002],[25.740009806000103,54.14625681600016],[24.37787072800009,53.88684112600008],[23.64261844900011,53.898985087000156],[23.485625448000064,53.939292704000124],[22.76721968600006,54.35626983700003],[21.267602287364923,55.24867008630527],[21.07439212300011,55.74494049700003]]]}}
#         )
#
#     def test_area(self):
#         obj = Area.objects.get(id=1)
#         self.assertEqual(obj.title, 'Testy')
#         self.assertEqual(obj.ccodes, ['ar'])
