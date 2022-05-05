pip download \
--only-binary :all: \
--platform manylinux1_x86_64 \
--python-version 38 \
-r requirements.txt \
-d deps

mkdir -p ./echo-function
 
cp -R deps ./echo-function/
cp -R src ./echo-function/
 
zip -r ./echo-function.zip ./echo-function