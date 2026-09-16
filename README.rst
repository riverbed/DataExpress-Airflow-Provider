.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.

Package ``apache-airflow-providers-dx``

Release: ``1.0.0``


`Riverbed Data Express (DX) <https://github.com/riverbed/DataExpress-Airflow-Provider>`__

Provider package
----------------

This is a provider package for ``dx`` provider. All classes for this provider package
are in ``airflow.providers.dx`` python package.

You can find package information and changelog for the provider in the
`documentation <docs/index.md>`_.

Installation
------------

You can install this package on top of an existing Airflow installation (see ``Requirements`` below
for the minimum Airflow version supported) via
``pip install apache-airflow-providers-dx``

The package supports the following python versions: 3.13

Requirements
------------

========================================== ==================
PIP package                                Version required
========================================== ==================
``apache-airflow``                         ``>=3.2.2,<3.3.0``
``apache-airflow-providers-dx``            ``1.0.0`` (this package)
``requests``                               ``>=2.28.0``
========================================== ==================

No other Airflow provider packages are required for DX DAGs (only ``airflow.providers.dx``).
Run an Airflow **Triggerer** process if you use deferrable DX sensors.

The changelog for the provider package can be found in the
`changelog <CHANGES.rst>`_.
