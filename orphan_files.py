import os
from django.conf import settings
from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "whg.settings")
application = get_wsgi_application()

from datasets.models import DatasetFile  # Adjust 'your_app' to the actual app name


def list_files(directory):
  """List all files in a directory recursively."""
  for root, dirs, files in os.walk(directory):
    for file in files:
      yield os.path.join(root, file)


def get_referenced_files():
  """Fetch all file paths from DatasetFile records."""
  files = DatasetFile.objects.all().values_list('file', flat=True)
  return set(files)


def find_unreferenced_files(media_root, referenced_files):
  """Find files that are not referenced in the DatasetFile records."""
  files_on_disk = set(list_files(media_root))
  unreferenced_files = files_on_disk - referenced_files
  return unreferenced_files


if __name__ == "__main__":
  media_root = os.path.join(settings.BASE_DIR, 'media')  # Adjust if your media root differs
  referenced_files = get_referenced_files()
  unreferenced_files = find_unreferenced_files(media_root, referenced_files)

  # Output unreferenced files for review
  print("Unreferenced Files:")
  for file_path in unreferenced_files:
    print(file_path)
