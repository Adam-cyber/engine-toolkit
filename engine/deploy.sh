

## ASSUMPTION:
##
## [1] sshuttle to dev db so that the localhost:5433 is the proxy for devdb
## e.g:
##
##JUMPBOX_USER=quynh.dang
##ssh -L 5433\:0.pg-edge.aws-dev-rt.veritone.com\:5432 -N ${JUMPBOX_USER}@0.jump.aws-dev-rt.veritone.com
##
##  [2] a push-xx has been done for the engines
# 

# This is what needs to be done:
#  (1) Get the build id of the build that was just pushed, e.g.  created_date time within 30' and build_state = available
#  (2) docker tag the build image to be the pattern of  ECR/dev-validated:${ENGINE_ID}:${BUILD_ID}
#  (3) docker push this tagged image
#  (4) update the database to move the current deployed build to paused and the new one to deployed
# UPDATE job_new.build SET build_state = paused WHERE build_state = deployed and engine_id=${ENGINE_ID}
# UPDATE job_new.build SET build_state = deployed WHERE build_id = ${BUILD_ID} and engine_id=${ENGINE_ID}
#
ENGINE_ID=$1
cat flipbuild.sql.template | sed  "s/ENGINE_ID/$1/g" > flipbuild.sql

DEVCORE=0.pg-core.aws-dev.veritone.com:5432

echo "\connect platform" > ${sqlfile}
echo "SELECT build_id FROM job_new.build WHERE build_state='available' and ENGINE_ID='${ENGINE_ID}' and created_date > extract(epoch from now())::int - 1800;" >> ${sqlfile}
psql -q -P format=unaligned -P tuples_only  -z -f $sqlfile -o $resfile postgres://postgres:postgres@${DEVCORE}

fieldValue:=`cat $resfile}`
echo export BUILD_ID=\'$fieldValue\'

