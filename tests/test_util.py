from unittest.mock import Mock

import pytest
from pytest_mock import MockerFixture

from spark_sight.util import is_latest_version


# TODO test
@pytest.mark.parametrize(
    "_version,_is_it",
    [
        ("0.1.3", True),
        ("0.1.4", False),
    ],
)
def no_test_is_latest_version(
    mocker: MockerFixture,
    _version: str,
    _is_it: bool,
):
    get_mock = Mock()
    response_mock = Mock()
    get_mock.return_value = response_mock
    response_mock.status_code = 200
    response_mock.text = '{"info":{"author":"Alfredo Fomitchenko","author_email":"alfredo.fomitchenko@mail.polimi.it","bugtrack_url":null,"classifiers":["Programming Language :: Python :: 3","Programming Language :: Python :: 3.6","Programming Language :: Python :: 3.7","Programming Language :: Python :: 3.8","Programming Language :: Python :: 3.9","Programming Language :: Python :: 3.10"],"description":"","description_content_type":null,"docs_url":null,"download_url":"","downloads":{"last_day":-1,"last_month":-1,"last_week":-1},"home_page":"","keywords":"","license":"","maintainer":"","maintainer_email":"","name":"spark-sight","package_url":"https://pypi.org/project/spark-sight/","platform":null,"project_url":"https://pypi.org/project/spark-sight/","project_urls":null,"release_url":"https://pypi.org/project/spark-sight/0.1.3/","requires_dist":["pandas (>=1.1,<=1.4.2)","plotly (>=5,<=5.7.0)"],"requires_python":">=3.6,<4.0","summary":"Spark performance at a glance","version":"0.1.3","yanked":false,"yanked_reason":null},"last_serial":13757644,"releases":{"0.1.0":[{"comment_text":"","digests":{"md5":"951f45d49ce9b570e0573bf38778ea01","sha256":"76e98cea76e375a9f61695dcbeb848691832b6539ec73d9399aab563d2194201"},"downloads":-1,"filename":"spark_sight-0.1.0-py3-none-any.whl","has_sig":false,"md5_digest":"951f45d49ce9b570e0573bf38778ea01","packagetype":"bdist_wheel","python_version":"py3","requires_python":">=3.6,<4.0","size":13413,"upload_time":"2022-05-07T08:14:19","upload_time_iso_8601":"2022-05-07T08:14:19.618802Z","url":"https://files.pythonhosted.org/packages/a3/de/430855580eeed6e6a39cfeea19875f6d04b2aea5dff76d02e5a7331eb5fe/spark_sight-0.1.0-py3-none-any.whl","yanked":false,"yanked_reason":null},{"comment_text":"","digests":{"md5":"1a2a04b9d395872c5b68ca579806144b","sha256":"bff00f956299401f0c9dbac75cb376613ee5626d3e53039e1fd3d7a8de7d3d3c"},"downloads":-1,"filename":"spark-sight-0.1.0.tar.gz","has_sig":false,"md5_digest":"1a2a04b9d395872c5b68ca579806144b","packagetype":"sdist","python_version":"source","requires_python":">=3.6,<4.0","size":10674,"upload_time":"2022-05-07T08:14:18","upload_time_iso_8601":"2022-05-07T08:14:18.047963Z","url":"https://files.pythonhosted.org/packages/af/81/3bad2b7e977513d3fef24d460b52a2c385bbea142a068e3daf3ba5450687/spark-sight-0.1.0.tar.gz","yanked":false,"yanked_reason":null}],"0.1.1":[{"comment_text":"","digests":{"md5":"9516885e49c5f1a337c8c1f2c41c94c2","sha256":"13bfef4559aca0a1fefe1c0bc2974cc7c7d873296eefc5c8c4f11533e670fca5"},"downloads":-1,"filename":"spark_sight-0.1.1-py3-none-any.whl","has_sig":false,"md5_digest":"9516885e49c5f1a337c8c1f2c41c94c2","packagetype":"bdist_wheel","python_version":"py3","requires_python":">=3.6,<4.0","size":13414,"upload_time":"2022-05-07T16:35:42","upload_time_iso_8601":"2022-05-07T16:35:42.069228Z","url":"https://files.pythonhosted.org/packages/8a/05/dab79da6c5d84100efa51c2a039f4e44a92c66db3485997f747f8d97771c/spark_sight-0.1.1-py3-none-any.whl","yanked":false,"yanked_reason":null},{"comment_text":"","digests":{"md5":"6582ed01ecf94b61edaf4971cb2ea262","sha256":"b5f4f0ff25c8adf8cbc8d18c00317c71aa5a2acdda51767e088c4eb9d3ed40f5"},"downloads":-1,"filename":"spark-sight-0.1.1.tar.gz","has_sig":false,"md5_digest":"6582ed01ecf94b61edaf4971cb2ea262","packagetype":"sdist","python_version":"source","requires_python":">=3.6,<4.0","size":10668,"upload_time":"2022-05-07T16:35:40","upload_time_iso_8601":"2022-05-07T16:35:40.455401Z","url":"https://files.pythonhosted.org/packages/24/19/ae0a9ff1d058fb7f3173493799d1511840d5f3f42827cf4db33b546c70b9/spark-sight-0.1.1.tar.gz","yanked":false,"yanked_reason":null}],"0.1.2":[{"comment_text":"","digests":{"md5":"b0823c312c7a2bb6e401ae8b7e6e679e","sha256":"3f9e68b83d8ef5610517882bb833b3a2485756df73b7c4639ea4f9abc5866d66"},"downloads":-1,"filename":"spark_sight-0.1.2-py3-none-any.whl","has_sig":false,"md5_digest":"b0823c312c7a2bb6e401ae8b7e6e679e","packagetype":"bdist_wheel","python_version":"py3","requires_python":">=3.6,<4.0","size":13414,"upload_time":"2022-05-09T07:16:50","upload_time_iso_8601":"2022-05-09T07:16:50.744100Z","url":"https://files.pythonhosted.org/packages/a9/be/ab65db093e7866d7bf6e76bdeeab4d255d3bbb44322c338b5af4168fafec/spark_sight-0.1.2-py3-none-any.whl","yanked":false,"yanked_reason":null},{"comment_text":"","digests":{"md5":"4079bda13739668777dd36a97bfea105","sha256":"5078cd5ce877c9f96e1b2453d339a28e4ddeb715252ee860afed9b58aba621a9"},"downloads":-1,"filename":"spark-sight-0.1.2.tar.gz","has_sig":false,"md5_digest":"4079bda13739668777dd36a97bfea105","packagetype":"sdist","python_version":"source","requires_python":">=3.6,<4.0","size":10680,"upload_time":"2022-05-09T07:16:49","upload_time_iso_8601":"2022-05-09T07:16:49.254697Z","url":"https://files.pythonhosted.org/packages/5f/aa/cb7166af7faf650a9110557deb5a2ca9e564d935f59ab3c02d1dbead425b/spark-sight-0.1.2.tar.gz","yanked":false,"yanked_reason":null}],"0.1.3":[{"comment_text":"","digests":{"md5":"da8a1189737642849a9f0e437f860650","sha256":"dc0d67716df8d59b59af879ffa0df167866defb634a7ec2b6b8f09dfa483a7ec"},"downloads":-1,"filename":"spark_sight-0.1.3-py3-none-any.whl","has_sig":false,"md5_digest":"da8a1189737642849a9f0e437f860650","packagetype":"bdist_wheel","python_version":"py3","requires_python":">=3.6,<4.0","size":13694,"upload_time":"2022-05-09T11:44:22","upload_time_iso_8601":"2022-05-09T11:44:22.217245Z","url":"https://files.pythonhosted.org/packages/23/55/4021f4c59627ecda76fbe3e131e8a38fef14f7f5327e0bc16ac5b3bca3b2/spark_sight-0.1.3-py3-none-any.whl","yanked":false,"yanked_reason":null},{"comment_text":"","digests":{"md5":"90563fb68c4052cf702e17276a2ac6d2","sha256":"9a1e9229fe0826d8dde22860f00dd2ffdc781ca232c9ed7d4c59510206cacbc4"},"downloads":-1,"filename":"spark-sight-0.1.3.tar.gz","has_sig":false,"md5_digest":"90563fb68c4052cf702e17276a2ac6d2","packagetype":"sdist","python_version":"source","requires_python":">=3.6,<4.0","size":10959,"upload_time":"2022-05-09T11:44:20","upload_time_iso_8601":"2022-05-09T11:44:20.470994Z","url":"https://files.pythonhosted.org/packages/13/03/5a1c9b89c23b20e8fbac227aa1ff8bfda179d700b1c7550fe60b2ef7c07c/spark-sight-0.1.3.tar.gz","yanked":false,"yanked_reason":null}]},"urls":[{"comment_text":"","digests":{"md5":"da8a1189737642849a9f0e437f860650","sha256":"dc0d67716df8d59b59af879ffa0df167866defb634a7ec2b6b8f09dfa483a7ec"},"downloads":-1,"filename":"spark_sight-0.1.3-py3-none-any.whl","has_sig":false,"md5_digest":"da8a1189737642849a9f0e437f860650","packagetype":"bdist_wheel","python_version":"py3","requires_python":">=3.6,<4.0","size":13694,"upload_time":"2022-05-09T11:44:22","upload_time_iso_8601":"2022-05-09T11:44:22.217245Z","url":"https://files.pythonhosted.org/packages/23/55/4021f4c59627ecda76fbe3e131e8a38fef14f7f5327e0bc16ac5b3bca3b2/spark_sight-0.1.3-py3-none-any.whl","yanked":false,"yanked_reason":null},{"comment_text":"","digests":{"md5":"90563fb68c4052cf702e17276a2ac6d2","sha256":"9a1e9229fe0826d8dde22860f00dd2ffdc781ca232c9ed7d4c59510206cacbc4"},"downloads":-1,"filename":"spark-sight-0.1.3.tar.gz","has_sig":false,"md5_digest":"90563fb68c4052cf702e17276a2ac6d2","packagetype":"sdist","python_version":"source","requires_python":">=3.6,<4.0","size":10959,"upload_time":"2022-05-09T11:44:20","upload_time_iso_8601":"2022-05-09T11:44:20.470994Z","url":"https://files.pythonhosted.org/packages/13/03/5a1c9b89c23b20e8fbac227aa1ff8bfda179d700b1c7550fe60b2ef7c07c/spark-sight-0.1.3.tar.gz","yanked":false,"yanked_reason":null}],"vulnerabilities":[]}'

    response_mock.encoding = "utf-8"
    
    codes_mock = Mock()
    codes_mock.ok = 200

    mocker.patch(
        "spark_sight.util.get",
        get_mock,
    )
    mocker.patch(
        "spark_sight.util.codes",
        codes_mock,
    )
    
    assert is_latest_version() == _is_it
