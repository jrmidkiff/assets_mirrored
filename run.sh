set -e

FILE="api_update.json"

export GIT_SSH_COMMAND="ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no"
git pull automated main
source ./venv/bin/activate
echo "Running assets' run.py"
python -u run.py --log=debug 

if git diff --exit-code $FILE &> /dev/null; then
    echo "No updates made to $FILE"
else 
    {
        echo "$FILE is modified"
        git add $FILE &&
        git commit -m "Updated $FILE" &&
        git push automated main     
    } || {
        echo "***** ERROR - No updates made to $FILE *****"
        exit 1
    }
fi

echo "Exiting"
deactivate
