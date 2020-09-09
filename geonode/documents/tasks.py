# -*- coding: utf-8 -*-
#########################################################################
#
# Copyright (C) 2017 OSGeo
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
#########################################################################

import os
from os import access, R_OK
from os.path import isfile

from geonode.celery_app import app
from celery.utils.log import get_task_logger

from geonode.documents.models import Document
from geonode.documents.renderers import render_document
from geonode.documents.renderers import generate_thumbnail_content
from geonode.documents.renderers import ConversionError
from geonode.documents.renderers import MissingPILError

from django.conf import settings
from django.core.files.storage import default_storage as storage

logger = get_task_logger(__name__)


@app.task(bind=True, queue='update')
def create_document_thumbnail(self, object_id):
    """
    Create thumbnail for a document.
    """
    logger.debug("Generating thumbnail for document #{}.".format(object_id))

    try:
        document = Document.objects.get(id=object_id)
    except Document.DoesNotExist:
        logger.error("Document #{} does not exist.".format(object_id))
        return

    document_path = os.path.join(settings.MEDIA_ROOT, document.doc_file.name)
    to_dispose = []

    try:
        if not os.path.exists(document_path):
            if storage.exists(document.doc_file.name):
                # Save document to expected local path temporarily
                parent = os.path.dirname(document_path)
                if not os.path.exists(parent):
                    os.makedirs(parent)

                f_src = storage.open(document.doc_file.name, 'rb')
                f_dst = open(document_path, 'wb+')
                buf = f_src.read(4096)

                while buf:
                    f_dst.write(buf)
                    buf = f_src.read(4096)

                f_dst.close()
                f_src.close()
                to_dispose.append(document_path)  # Dispose of temp doc when done
            else:
                logger.error("Document #{} exists but its path could not be resolved.".format(object_id))
                return

        image_path = None

        if document.is_image():
            image_path = document_path
        elif document.is_file():
            try:
                image_path = render_document(document_path)
                to_dispose.append(image_path)
            except ConversionError as e:
                logger.debug("Could not convert document #{}: {}."
                            .format(object_id, e))

        try:
            if image_path:
                assert isfile(image_path) and access(image_path, R_OK) and os.stat(image_path).st_size > 0
        except (AssertionError, TypeError):
            image_path = None

        if not image_path:
            image_path = document.find_placeholder()

        if not image_path or not os.path.exists(image_path):
            logger.debug("Could not find placeholder for document #{}"
                        .format(object_id))
            return

        thumbnail_content = None
        try:
            thumbnail_content = generate_thumbnail_content(image_path)
        except MissingPILError:
            logger.error('Pillow not installed, could not generate thumbnail.')
            return

        if not thumbnail_content:
            logger.warning("Thumbnail for document #{} empty.".format(object_id))
        filename = 'document-{}-thumb.png'.format(document.uuid)
        document.save_thumbnail(filename, thumbnail_content)
        logger.debug("Thumbnail for document #{} created.".format(object_id))
    finally:
        for _path in to_dispose:
            os.remove(_path)


@app.task(bind=True, queue='cleanup')
def delete_orphaned_document_files(self):
    from geonode.documents.utils import delete_orphaned_document_files
    delete_orphaned_document_files()


@app.task(bind=True, queue='cleanup')
def delete_orphaned_thumbnails(self):
    from geonode.base.utils import delete_orphaned_thumbs
    delete_orphaned_thumbs()
