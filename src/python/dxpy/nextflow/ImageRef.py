#!/usr/bin/env python

import os

class ImageRef(object):
    def __init__(
            self,
            process,
            digest,
            dx_file_id=None,
            repository=None,
            image_name=None,
            tag=None
    ):
        """
        A class to handle an image reference from nextflow pipeline.
        :param process: An NPA proces name (aka task name) which uses a given image
        :type process: String
        :param digest: An image digest
        :type digest: String
        :param dx_file_id: dx file id on the platform
        :type dx_file_id: Optional[String]
        :param repository: Image repository
        :type repository: Optional[String]
        :param image_name: Image name (usually a basename of the image referenced with repository)
        :type image_name: Optional[String]
        :param tag: A version tag
        :type tag: Optional[String]
        """
        self._caching_dir = os.path.join("/.cached_docker_images/", image_name)
        self._bundled_depends = None

    def cache(self):
        """
        Function to store an image on the platform as a dx file object. Should be implemented in subclasses.
        :returns: dx file id
        """
        raise NotImplementedError("Abstract class. Method not implemented. Use the concrete implementations.")

    @property
    def bundled_depends(self):
        if not self._bundled_depends:
            self._bundled_depends = self._package_bundle()
        return self._bundled_depends

    def _package_bundle(self):
        """
        Function to include a container image stored on the platform into NPA
        :returns: Dict in the format of {"name": "bundle.tar.gz", "id": {"$dnanexus_link": "file-xxxx"}}
        """
        return {}


class DockerImageRef(ImageRef):
    def __init__(
            self,
            process,
            digest,
            dx_file_id=None,
            repository=None,
            image_name=None,
            tag=None
    ):
        super().__init__(
            process,
            digest,
            dx_file_id,
            repository,
            image_name,
            tag)

    def cache(self):
        # docker pull, docker save, dx upload.
        # return file id
        return None

