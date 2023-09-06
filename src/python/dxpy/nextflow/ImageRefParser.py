#!/usr/bin/env python

import re

class ImageRefParserFactoryError(Exception):
    """
    An exception class to handle image reference regex patterns
    """


class ImageRefParserFactory(object):
    def __init__(self, image_ref):
        """
        :param image_ref: Image reference
        :type image_ref: str
        A factory class to determine which type of Image reference was used.
        """
        self._image_ref = image_ref
        dx_prefix_scheme = "dx"
        self._DX_URI_PATTERN = re.compile("^" + dx_prefix_scheme + "://(([^/]+):)?(.*)$")

        # Pattern matching a docker image reference
        # https://stackoverflow.com/questions/39671641/regex-to-parse-docker-tag with group modifications for regex below

        repository_group = "((?:(?=[^:\\/]{1,253})(?!-)[a-zA-Z0-9-]{1,63}(?<!-)(?:\\.(?!-)[a-zA-Z0-9-]{1,63}(?<!-))*(?::[0-9]{1,5})?/)*)"
        image_name_group = "((?![._-])(?:[a-z0-9._-]*)(?<![._-])(?:/(?![._-])[a-z0-9._-]*(?<![._-]))*)"
        tag_or_digest_group = "((?::(?![.-])[a-zA-Z0-9_.-]{1,128})?|(?:@sha256:([0-9a-f]{64})$))"
        self._DOCKER_IMAGE_PATTERN = re.compile("^" + repository_group + image_name_group + tag_or_digest_group + "$")

        self._tokens = None

    @property
    def parse(self):
        if not self._tokens:
            self._tokens = self._get_tokens()
        return  self._tokens

    def _get_tokens(self):
        dx_regex_matcher = self._DX_URI_PATTERN.search(self._image_ref)
        docker_matcher = self._DOCKER_IMAGE_PATTERN.search(self._image_ref)
        if dx_regex_matcher:
            return DxPathParser(dx_regex_matcher)
        elif docker_matcher:
            return DockerImageParser(docker_matcher)
        else:
            raise ImageRefParserFactoryError(
                "URI does not match the dx uri pattern, nor the docker image name pattern: {}".format(self._image_ref)
            )



class ImageRefParser(object):
    def __init__(self, regex_matcher):
        self._regex_matcher = regex_matcher

    def _parse(self):
        raise NotImplementedError("Abstract class. Method not implemented. Use the concrete implementations.")


class DxPathParser(ImageRefParser):
    def __init__(self, regex_matcher):
        """
        :param regex_matcher: regex match of a given image reference
        :type regex_matcher: re.Pattern
        A class to represent parts of a dx file path or file ID
        """
        super().__init__(regex_matcher)
        self._file_id_regex = re.compile("file-[A-Za-z0-9]{24}")
        self.name = None
        self.context_id = None
        self.file_path = None
        self.file_id = None
        self._parse()

    def _parse(self):
        self.context_id = self.name = self._regex_matcher.group(2)
        extracted_path = self._regex_matcher.group(3)
        if self._file_id_regex.search(extracted_path):
            self.file_id = extracted_path
        else:
            self.file_path = extracted_path
        return None


class DockerImageParser(ImageRefParser):
    def __init__(self, regex_matcher):
        super().__init__(regex_matcher)
        self.repository = None
        self.image = None
        self.tag = None
        self.digest = None
        self._parse()

    def _parse(self):
        self.repository = self._regex_matcher.group(1)
        self.image = self._regex_matcher.group(2)
        if self._regex_matcher.group(3).startswith("@sha"):     # last match group is a digest
            self.digest = self._regex_matcher.group(3)[1:]
            self.tag = ""

        elif self._regex_matcher.group(3).startswith(":"):      # last match group is a version tag
            self.tag = self._regex_matcher.group(3)[1:]
            self.digest = ""

        else:                                                   # last match group not present
            self.digest = self._regex_matcher.group(3)
            self.tag = self._regex_matcher.group(3)

