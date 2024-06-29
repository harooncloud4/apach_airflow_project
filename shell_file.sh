#!/usr/bin/bash
# ============================================================================
# Name:       {filename}.sh
# Purpose:    {Description of project}
# Location:   /home/portaladmin/scripts/{dev/uat/stage/live}/python/Data_Science/{path_to_project}/{filename}.sh
# Usage:      {How the script will run - manually, via cron, or is being called from another script}
# Virtual Env:/home/portaladmin/scripts/{dev/uat/stage/live}/python/venvs/{virtual_environment_name}
# Parameters: {Any arguments required for the script to run}
# 
# Amendment History:
#
# Name           Date       Ver  Desc
# ============== ========== ==== =============================================
# {Your name}    {the date} 1.0  Initial Release
# Tushar Palawat 15-04-2024 2.0  Changed the shell file to use the new servers which automatically chooses the correct environment
# Tushar Palawat 22-04-2024 2.1  Took all the common functions and put them in ip_functions.sh. And calling those functions here in this script.
#
# ============================================================================

# change these two lines to run your project
PROJECT_NAME="<Unique name for your project - will be the name of your virtual environment. Keep it small case, and join spaces with underscore (_)>+"
PATH_TO_PROJECT="${DATA_SCIENCE_BASE}/<Define_The_Path_To_The_Project_in_Data_Science_Repository>"

#Source in Standard Environment and Functions
source ${SH_BASE}/ip_functions.sh
# cd'ing into the project directory
cd $PATH_TO_PROJECT

# The virtual environments are saved in a separate 'venvs' subdirectory in 'the environment' directory, instead of being saved withing the project
# Specifying the virtual environment directory and creating it if doesn't already exist
ENV_DIR="${VENV_BASE}/${PROJECT_NAME}"
create_virtual_environment $ENV_DIR

# install requirements for the project in a new virtual environment
install_requirements $ENV_DIR $PATH_TO_PROJECT

# Run the Python Scripts
run_python_script $PROJECT_NAME $PATH_TO_PROJECT $WORK2

# runs the logmsg command to log the python run
log_python_run $WORK2

# if any errors occured during python run, sends an error mesage out
send_error_log $PROG $LOG

echo "This has worked and completed running"
logmsg "This has worked and completed running"