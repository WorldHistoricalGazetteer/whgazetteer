import logging, codecs, json, os, re, sys
from frictionless import validate as fvalidate
from jsonschema import draft7_format_checker, validate, ValidationError
from .exceptions import LPFValidationError, DelimValidationError

def parse_validation_error(error):
    # Extract key parts of the error message
    # print('parse_validation_error()', error)
    data = error.instance
    message = error.message
    schema_path = " -> ".join([str(p) for p in error.absolute_schema_path])

    # Construct a user-friendly message
    user_message = f"Error in data: {data}. Reason: {message}. Schema path: {schema_path}"

    return user_message

#
# validate Linked Places json-ld (w/jsonschema)
# format ['coll' (FeatureCollection) | 'lines' (json-lines)]
# TODO: 'format' will eventually support jsonlines
def validate_lpf(tempfn, format):
	logger = logging.getLogger('django')
	# print('in validate_lpf()...format', format)
	schema = json.loads(codecs.open('datasets/static/validate/schema_lpf_v1.2.json','r','utf8').read())

	# rename tempfn
	newfn = tempfn+'.jsonld'
	os.rename(tempfn,newfn)
	infile = codecs.open(newfn, 'r', 'utf8')
	result = {"format":"lpf", "errors":[]}

	# TODO: handle json-lines
	jdata = json.loads(infile.read())
	if len(set(['type', '@context', 'features'])-set(jdata.keys())) > 0 \
		or jdata['type'] != 'FeatureCollection' \
		or len(jdata['features']) == 0:
		print('not valid GeoJSON-LD')
	else:
		errors = []
		seen_error_paths = set()

		for countrows, feat in enumerate(jdata['features'], start=1):

			if len(errors) >= 3:  # Stop after collecting 3 errors
				break
			try:
				validate(
					instance=feat,
					schema=schema,
					format_checker=draft7_format_checker
				)
			except ValidationError as e:
				error_path = " -> ".join([str(p) for p in e.absolute_path])
				if error_path not in seen_error_paths:  # Check if this error type (path) has been seen before
					detailed_error = parse_validation_error(e)
					errors.append({"feat": countrows, 'error': detailed_error})
					seen_error_paths.add(error_path)

		print('errors in validate_lpf()', errors)
		if errors:
			aggregated_message = "; ".join([error['error'] for error in errors])
			if len(errors) == 3:
				aggregated_message += " ... Your uploaded file has more errors; these were the first three found."
			# print('aggregated_message',aggregated_message )
			raise LPFValidationError(errors)
			# raise LPFValidationError(aggregated_message)

		result['count'] = countrows
	return result

#
# replaces validate_tsv()
def validate_delim(df):
	errors = []

	# Define required fields and patterns
	required_fields = ['id', 'title', 'title_source', 'start']
	pattern_constraints = {
		'ccodes': "([a-zA-Z]{2};?)+",
		'matches': "(https?:\\/\\/.*\\..*;?)+|([a-z]{1,8}:.*;?)+",
		'parent_id': "(https?:\/\/.*\\..*|#\\d*)",
		'start': "(-?\\d{1,4}(-\\d{2})?(-\\d{2})?)(\/(-?\\d{1,4}(-\\d{2})?(-\\d{2})?))?",
		'end': "(-?\\d{1,4}(-\\d{2})?(-\\d{2})?)(\/(-?\\d{1,4}(-\\d{2})?(-\\d{2})?))?"
	}
	range_constraints = {
		'lon': (-180, 180),
		'lat': (-90, 90)
	}

	# Loop through rows for validation
	for index, row in df.iterrows():
		# Check required fields
		for field in required_fields:
			if field not in row:
				errors.append({"row": index + 1, "error": f"Required field missing: {field}"})

		# Check for either "parent_name" or "parent_id"
		if not ("parent_name" in row or "parent_id" in row):
			errors.append({"row": index + 1, "error": "Either 'parent_name' or 'parent_id' must be present"})

		# Check pattern constraints
		for field, pattern in pattern_constraints.items():
			if field in row and not bool(re.search(pattern, str(row[field]))):
				errors.append(
					{"row": index + 1, "error": f"Field {field} contains a value that does not match the required pattern"})

		# Check range constraints
		for field, (low, high) in range_constraints.items():
			if field in row and (row[field] < low or row[field] > high):
				errors.append({"row": index + 1, "error": f"Value in {field} is out of the allowed range"})

	if errors:
		raise DelimValidationError(errors)

	return errors

#
# DEPRECATED 2023-08 validate LP-TSV file (uses frictionless.py 3.31.0)
#
def validate_tsv(fn, ext):
	# incoming csv or tsv; in cases converted from xlsx or ods via pandas
	# print('validate_tsv() fn', fn)
	# pull header for missing columns test below
	header = codecs.open(fn, 'r').readlines()[0][:-1]
	header = list(map(str.lower, header.split('\t' if '\t' in header else ',')))
	# header = header.split('\t' if '\t' in header else ',')
	list(map(str.lower, header))
	# print('header', header)
	result = {"format":"delimited", "errors":[], "columns":header}
	schema_lptsv = json.loads(codecs.open('datasets/static/validate/schema_tsv.json', 'r', 'utf8').read())
	try:
		report = fvalidate(fn, schema=schema_lptsv, sync_schema=True)
	except:
		err = sys.exc_info()
		result['errors'].append('File failed format validation. Error: '+err+'; '+str(err[1].args))
		print('error on fvalidate',err)
		print('error args',err[1].args)
		return result

	if len(report['tables']) > 0:
		rpt = report['tables'][0]
		result['count'] = rpt['stats']['rows']  # count
		print('rpt errors', rpt['errors'])

	req = ['id', 'title', 'title_source', 'start']
	missing = list(set(req) - set(header))

	# filter harmless errors
	# TODO: is filtering encoding-error here problematic?
	result['errors'] = [x['message'] for x in rpt['errors'] \
	                    if x['code'] not in ["blank-header", "missing-header", "encoding-error"]]
	if len(missing) > 0:
		result['errors'].insert(0,'Required column(s) missing or header malformed: '+
		                        ', '.join(missing))

	return result