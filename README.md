# amundsengremlin
Amundsen Gremlin

## Instructions to configure venv
Virtual environments for python are convenient for avoiding dependency conflicts.
The `venv` module built into python3 is recommended for ease of use, but any managed virtual environment will do.
If you'd like to set up venv in this repo:
```bash
$ venv_path=[path_for_virtual_environment]
$ python3 -m venv $venv_path
$ source $venv_path/bin/activate
$ pip install -r requirements.txt
```

If something goes wrong, you can always:
```bash
$ rm -rf $venv_path
```