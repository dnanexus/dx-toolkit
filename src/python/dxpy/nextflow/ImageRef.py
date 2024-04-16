#!/usr/bin/env python

import os
import subprocess

from dxpy import DXFile, config, upload_local_file
from dxpy.exceptions import err_exit


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
        self._caching_dir = os.path.join("/.cached_docker_images/", image_name or "")
        self._dx_file_id = dx_file_id
        self._bundled_depends = None
        self._repository = repository
        self._image_name = image_name
        self._tag = tag
        self._digest = digest
        self._process = process

    @property
    def bundled_depends(self):
        if not self._bundled_depends:
            self._bundled_depends = self._package_bundle()
        return self._bundled_depends

    @property
    def identifier(self):
        return self._join_if_exists("_", [self._repository, self._image_name, self._tag, self._digest])

    def _cache(self, file_name):
        """
        Function to store an image on the platform as a dx file object. Should be implemented in subclasses.
        :param file_name: A file name under which the image will be saved on the platform
        :type file_name: String
        :returns: Tuple[String, String] dx file id, file name (basename)
        """
        raise NotImplementedError("Abstract class. Method not implemented. Use the concrete implementations.")

    def _reconstruct_image_ref(self):
        raise NotImplementedError("Abstract class. Method not implemented. Use the concrete implementations.")

    def _construct_cache_file_name(self):
        return self._join_if_exists("_", [self._image_name, self._tag])

    @staticmethod
    def _join_if_exists(delimiter, parts):
        return delimiter.join([x for x in parts if x])

    def _dx_file_get_name(self):
        dx_file_handle = DXFile(self._dx_file_id, config["DX_PROJECT_CONTEXT_ID"])
        return dx_file_handle.describe().get("name")

    def _package_bundle(self):
        """
        Function to include a container image stored on the platform into NPA
        :returns: Dict in the format of {"name": "bundle.tar.gz", "id": {"$dnanexus_link": "file-xxxx"}}
        """
        if not self._dx_file_id:
            cache_file_name = self._construct_cache_file_name()
            self._dx_file_id = self._cache(cache_file_name)
        else:
            cache_file_name = self._dx_file_get_name()
        return {
            "name": cache_file_name,
            "id": {"$dnanexus_link": self._dx_file_id}
        }


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

    def _cache(self, file_name):
        full_image_ref = self._reconstruct_image_ref()
        docker_pull_cmd = "sudo docker pull {}".format(full_image_ref)
        docker_save_cmd = "sudo docker save {} | gzip > {}".format(full_image_ref, file_name)
        for cmd in [docker_pull_cmd, docker_save_cmd]:
            try:
                _ = subprocess.check_output(cmd, shell=True)
            except subprocess.CalledProcessError:
                err_exit("Failed to run a subprocess command: {}".format(cmd))
        # may need wait_on_close = True??
        extracted_digest = self._digest
        if not self._digest:
            digest_cmd = "docker images --no-trunc --quiet {}".format(full_image_ref)
            extracted_digest = subprocess.check_output(digest_cmd, shell=True).decode().strip()
        uploaded_dx_file = upload_local_file(
            filename=file_name,
            project=config["DX_PROJECT_CONTEXT_ID"],
            folder=self._caching_dir,
            name=file_name,
            parents=True,
            properties={"image_digest": self._digest or extracted_digest}
        )
        return uploaded_dx_file.get_id()

    def _reconstruct_image_ref(self):
        """
        Docker image reference has the form of <REPOSITORY_NAME>/<IMAGE_NAME>:<VERSION_TAG> or
        <REPOSITORY_NAME>/<IMAGE_NAME>@<DIGEST>
        """
        repo_and_image_name = self._join_if_exists("", [self._repository, self._image_name])
        if self._digest:
            full_ref = self._join_if_exists("@", [repo_and_image_name, self._digest])
        else:
            full_ref = self._join_if_exists(":", [repo_and_image_name, self._tag])
        return full_ref
