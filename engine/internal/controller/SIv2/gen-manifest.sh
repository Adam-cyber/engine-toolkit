BUILD_DATE=$(date)
GIT_REPO=$(basename `git rev-parse --show-toplevel`)
GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
GIT_COMMIT=$(git rev-parse HEAD)
GIT_AUTHOR=$(git log --format='%aN' ${GIT_COMMIT}^!)

RELEASE_NOTES="${BUILD_DATE};user=${GIT_AUTHOR};repo=${GIT_REPO};branch=${GIT_BRANCH};commit=${GIT_COMMIT}"
echo $RELEASE_NOTES
sed "s#RELEASE_NOTES#$RELEASE_NOTES#g" manifest.template > manifest.json.generated
