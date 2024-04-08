#!/usr/bin/env python

from dxpy.nextflow.ImageRef import DockerImageRef


class ImageRefFactoryError(Exception):
    """
    Class to handle errors with instantiation of ImageRef subclasses
    """


class ImageRefFactory(object):
    def __init__(
            self,
            image_ref
    ):
        """
        A class to instantiate subclasses of ImageRef based on the container engine. Ususally instantiated after using the
        nextaur:collect function to collect images for docker and other container engines.
        :param image_ref: Image ref details
        :type image_ref: Dict
        """
        self._image_ref = image_ref
        self._engine = image_ref.get("engine", None)
        if not self._engine:
            raise ImageRefFactoryError("Provide the container engine")
        self._imageRef_switch = {
            "docker": DockerImageRef
        }

    def get_image(self):
        image = self._imageRef_switch.get(self._engine, None)
        if not image:
            raise ImageRefFactoryError("Unsupported container engine: {}".format(self._engine))
        return image(
            process=self._image_ref["process"],
            digest=self._image_ref["digest"],
            dx_file_id=self._image_ref.get("file_id", None),
            repository=self._image_ref.get("repository", None),
            image_name=self._image_ref.get("image_name", None),
            tag=self._image_ref.get("tag", None)
        )

