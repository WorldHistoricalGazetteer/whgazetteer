# permissions for Collection objects

def can_edit_collection(user, collection):
    return user == collection.owner or \
           user in collection.collaborators.all() or \
           user.groups.filter(name='whg_team').exists() or \
           user.groups.filter(name='whg_admins').exists()

def can_delete_collection(user, collection):
    return user == collection.owner or \
           user.groups.filter(name='whg_admins').exists()

def can_add_collaborator(user, collection):
  result = user == collection.owner or \
         user.groups.filter(name='whg_admins').exists()
  print(f"Checking can_add_collaborator for user {user} and collection {collection}: Result is {result}")
  return result
