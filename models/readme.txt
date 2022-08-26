This directory is an example of default models directories structure:

models/bin/
  default location of model executables and model.sqlite database files.
  To change default location use: -oms.ModelDir

models/log/
  default location of model run log files, to change it use: -oms.ModelLogDir

models/home/io/download
models/home/io/upload
  it is optional
  DO NOT create such directories on public web-sites, never.
  use it only for authenticated users and only if you allow to download or upload model data.
  default location of user data, model downloads and model uploads.
  To enable downloads or uploads use: -oms.AllowDownload -oms.AllowUpload
  To change deafult location use:     -oms.HomeDir

For example:

cd openmpp-root-dir
bin/oms -oms.HomeDir models/home2 -oms.AllowDownload -oms.AllowUpload
