# 3KLES-MONGODBUTIL

This package is based on:
- [3kles@/3kles-corebe](https://www.npmjs.com/package/@3kles/3kles-corebe)
- [3kles@/3kles-coremongodb](https://www.npmjs.com/package/@3kles/3kles-coremongodb)
- [3kles@/3kles-amqpbroker](https://www.npmjs.com/package/@3kles/3kles-amqpbroker)

This toolbox to easily create mongodbservice with selection and event.

## Services

**MongoDBSelectionService** is a service based on **MongoDBService** to add selection and event

- **getSelected**: Get selected records
- **selectAll**: Select all records
- **select**: Select record by id
- **clearSelection**: Clear current selection
- **deleteSelection**: Delete selected records
  
## Routes

**MongoDBSelectionRouter** is a service based on **MongoDBRouter** to add selection and event
Add routes for:
- **getSelected**
- **selectAll**
- **select**
- **clearSelection**
- **deleteSelection**

## Utils

**GenericCRUDEvent** is a class to simplify event transmission:

- **emit** (data:any, type:string): Method to send message (data) to exchange with routingKey (type)
- **create**: Used when creating record
- **update**: Used when updating record
- **delete**: Used when deleting record

**CRUDSelectionUtil** is a toolbox
- **buildGenericRoute**: Create a route from model and GenericCRUDEvent
- **generate**: Override **CRUDUtil @3Kles/3kles-corebe** to add select, selectAll, clearSelection