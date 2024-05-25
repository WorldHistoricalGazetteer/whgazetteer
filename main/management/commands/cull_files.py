import os
import shutil
from django.core.management.base import BaseCommand
from datasets.models import (DatasetFile)

class Command(BaseCommand):
    help = 'Check and clean up files in /media/user_* directories'

    def handle(self, *args, **kwargs):
        media_root = 'media'
        user_dirs = [os.path.join(media_root, d) for d in os.listdir(media_root)
                     if d.startswith('user_') and os.path.isdir(os.path.join(media_root, d))]
        print('user_dirs', user_dirs)
        file_count = 0
        orphaned_files = []
        total_files_size = 0
        total_files_count = 0

        for user_dir in user_dirs:
            for root, _, files in os.walk(user_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    file_count += 1
                    total_files_count += 1
                    total_files_size += os.path.getsize(file_path) / (1024 * 1024)  # convert bytes to megabytes

                    # Check if the file corresponds to a dataset_file record
                    relative_file_path = os.path.relpath(file_path, media_root)
                    if not DatasetFile.objects.filter(file=relative_file_path).exists():
                        orphaned_files.append(file_path)
                    # if not DatasetFile.objects.filter(file=file_path).exists():
                    #     orphaned_files.append(file_path)

        self.stdout.write(self.style.SUCCESS(f'Total files: {file_count}'))
        self.stdout.write(self.style.WARNING(f'Orphaned files: {len(orphaned_files)}'))
        self.stdout.write(self.style.SUCCESS(f'Total files count: {total_files_count}'))
        self.stdout.write(self.style.SUCCESS(f'Total files size: {total_files_size} MB'))

        for orphaned_file in orphaned_files:
        #     self.stdout.write(self.style.WARNING(f'Orphaned file: {orphaned_file}'))
            # Uncomment the lines below to move the orphaned files to media/orphans and zip them
            shutil.move(orphaned_file, 'media/orphans')
            shutil.make_archive('media/orphans', 'zip', 'media/orphans')
        self.stdout.write(self.style.SUCCESS('zip created successfully'))

        self.stdout.write(self.style.SUCCESS('File check and clean up completed'))
# class Command(BaseCommand):
#     help = 'Check and clean up files in /media/user_* directories'
#
#     def handle(self, *args, **kwargs):
#         media_root = 'media'
#         user_dirs = [os.path.join(media_root, d) for d in os.listdir(media_root) if d.startswith('user_') and os.path.isdir(os.path.join(media_root, d))]
#         print('user_dirs', user_dirs)
#         file_count = 0
#         orphaned_files = []
#
#         for user_dir in user_dirs:
#             for root, _, files in os.walk(user_dir):
#                 for file in files:
#                     file_path = os.path.join(root, file)
#                     file_count += 1
#
#                     # Check if the file corresponds to a dataset_file record
#                     if not DatasetFile.objects.filter(file=file_path).exists():
#                         orphaned_files.append(file_path)
#
#         self.stdout.write(self.style.SUCCESS(f'Total files: {file_count}'))
#         self.stdout.write(self.style.WARNING(f'Orphaned files: {len(orphaned_files)}'))
#
#         for orphaned_file in orphaned_files:
#             self.stdout.write(self.style.WARNING(f'Orphaned file: {orphaned_file}'))
#             # Uncomment the line below to delete the orphaned files
#             # os.remove(orphaned_file)
#
#         self.stdout.write(self.style.SUCCESS('File check and clean up completed'))
