.. _plugins:

============
Introduction
============

Tutor comes with a plugin system that allows anyone to customise the deployment of an Open edX platform very easily. The vision behind this plugin system is that users should not have to fork the Tutor repository to customise their deployments. For instance, if you have created a new application that integrates with Open edX, you should not have to describe how to manually patch the platform settings, ``urls.py`` or ``*.env.yml`` files. Instead, you can create a "tutor-myapp" plugin for Tutor. Then, users will start using your application in three simple steps::

    # 1) Install the plugin
    pip install tutor-myapp
    # 2) Enable the plugin
    tutor plugins enable myapp
    # 3) Reconfigure and restart the platform
    tutor local quickstart

For simple changes, it may be extremely easy to create a Tutor plugin: even non-technical users may get started with our :ref:`plugin_development_tutorial` tutorial. We also provide a list of :ref:`simple example plugins <plugins_examples>`.

Plugin commands cheatsheet
==========================

List installed plugins::

    tutor plugins list

Enable/disable a plugin::

    tutor plugins enable myplugin
    tutor plugins disable myplugin

After enabling or disabling a plugin, the environment should be re-generated with::

    tutor config save

The full plugins CLI is described in the :ref:`reference documentation <cli_plugins>`.

.. _existing_plugins:

Existing plugins
================

Officially-supported plugins are listed on the `Overhang.IO <https://overhang.io/tutor/plugins>`__ website.
