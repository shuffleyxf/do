from do_log import *


def test_set_log_file():
    file_path = os.path.join(os.getcwd(), 'do.log')
    if os.path.isfile(file_path):
        os.remove(file_path)

    assert not os.path.isfile(file_path)
    set_log_file(file_path)
    info("hello")
    assert os.path.isfile(file_path)
