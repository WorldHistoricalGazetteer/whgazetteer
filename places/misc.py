from django.shortcuts import get_object_or_404, render, redirect
from places.models import *
from shapely.geometry import MultiPolygon, MultiLineString, Point, mapping

# river
r=get_object_or_404(Place,id=103060).geoms.first()
rcoords = r.json['coordinates']
river = MultiLineString(rcoords)
river.is_valid
hully=river.convex_hull
hull_dict=mapping(hully)
#gj=json.loads(json.dumps(hull_dict).replace("\'", "\""))
gj=json.loads(json.dumps(hull_dict))
print(river.length)

