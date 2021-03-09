import codecs, csv, os
wd='/users/karlg/Documents/Repos/_whgazetteer/_testdata/'
tempfn = wd+'curacao.tsv'
from datasets.models import Dataset
from datasets.utils import *
from places.models import *
from django.shortcuts import get_object_or_404
ds = get_object_or_404(Dataset,pk=942)
p = get_object_or_404(Place, pk=6593333)

dsf = ds.files.all().order_by('-rev')[0]
infile = dsf.file.open(mode="r")
reader = csv.reader(infile, delimiter=dsf.delimiter)

infile.seek(0)
header = next(reader, None)
# strip BOM character if exists
header[0] = header[0][1:] if '\ufeff' in header[0] else header[0]
#header = header if type(header) = list else 
print('header', header)

r = next(reader, None)
