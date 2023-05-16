# Introduction

## Creating a workflow file

Annatto workflow files describe which importers, manipulators and exporters to execute.
For now, we use an XML file with the ending `.ato`, that is inspired by the Pepper workflow format.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<pepper-job version="1.0">
    <importer name="create_empty_corpus" path="." />
    <manipulator name="check">
        <property key="config.path">check_empty_corpus.csv</property>
    </manipulator>
    <exporter name="export_graphml" path="./">
  </exporter>
</pepper-job>
```

We will switch to a TOML based format soon.

## Modules

Annatto comes with a number of modules, which have different types:

**Importer** modules allow importing files from different formats. 
More than one importer can be used in a workflow, but then the corpus data needs
to be merged using one of the merger manipulators.
When running a workflow, the importers are executed first and in parallel.


**Manipulator** modules change the imported corpus data. 
They are executed one after another (non-parallel) and in the order they have
been defined in the workflow.

**Exporter** modules export the data into different formats.
More than one exporter can be used in a workflow.
When running a workflow, the exporters are executed last and in parallel.