#
# abbreviated place attribute lists from queryset
# for portal display
#
def attribListFromSet(attr, qs):
  attrib_list = []
  for item in qs:
    label = item.jsonb.get('toponym') if attr == 'names' \
      else item.jsonb.get('sourceLabel', item.jsonb.get('source_label', 'none'))
    if 'when' in item.jsonb:
      obj = {
        "label": label,
        "timespans": [[int(t['start'][list(t['start'].keys())[0]]),
                       int(t['end'][list(t['end'].keys())[0]])
                       if 'end' in t else
                       int(t['start'][list(t['start'].keys())[0]])]
                      for t in item.jsonb['when']['timespans']]
      }
      print(obj)
    else:
      obj = {"label": label}
    attrib_list.append(obj)
  return attrib_list

# def attribListFromSet(attr,qs):
#   attrib_list=[]
#   value='toponym' if attr=='names' else 'sourceLabel'
#   for item in qs:
#     if 'when' in item.jsonb:
#       obj={
#         "label":item.jsonb['toponym'] if attr=='names' \
#         else item.jsonb['sourceLabel'],
#         "timespans": [[int(t['start'][list(t['start'].keys())[0]]),
#                        int(t['end'][list(t['end'].keys())[0]])
#                        if 'end' in t else
#                        int(t['start'][list(t['start'].keys())[0]])] \
#                         for t in item.jsonb['when']['timespans']]
#             }
#       print(obj)
#     else:
#       # Check for both 'sourceLabel' and 'source_label'
#       label = item.jsonb['toponym'] if attr == 'names' \
#         else item.jsonb.get('sourceLabel', item.jsonb.get('source_label', ''))
#       obj = {"label": label}
#     # else:
#     #   obj={"label":item.jsonb['toponym'] if attr=='names' \
#     #     else item.jsonb['sourceLabel']}
#     attrib_list.append(obj)
#   return attrib_list


# def attribListFromSet(attr, qs):
# 	attrib_list = []
# 	value = 'toponym' if attr == 'names' else 'sourceLabel'
# 	for item in qs:
# 		if 'when' in item.jsonb:
# 			obj = {
# 				"label": item.jsonb['toponym'] if attr == 'names' \
# 					else item.jsonb['sourceLabel'],
# 				"timespans": [[int(t['start'][list(t['start'].keys())[0]]),
# 				               int(t['end'][list(t['end'].keys())[0]])
# 				               if 'end' in t else
# 				               int(t['start'][list(t['start'].keys())[0]])] \
# 				              for t in item.jsonb['when']['timespans']]
# 			}
# 			print(obj)
# 		else:
# 			obj = {"label": item.jsonb['toponym'] if attr == 'names' \
# 				else item.jsonb['sourceLabel']}
# 		attrib_list.append(obj)
# 	return attrib_list

# ', '.join(item.jsonb['label'].split(', ')[:3])
