#!/bin/sh
mysqldump \
-u root \
-h mysql \
-p \
--databases example \
--tables address \
--no-create-db \
--no-create-info \
--skip-opt \
--skip-add-drop-table \
--skip-comments \
--compress \
--quick \
--single-transaction
