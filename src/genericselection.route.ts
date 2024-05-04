import { MongoDBController, MongoDBRouter } from "@3kles/3kles-coremongodb";

export class MongoDBSelectionRouter extends MongoDBRouter {

	public addController(controller: MongoDBController): void {
		this.router.route('/' + controller.getParameters().modelname + '/getSelected').get(controller.execute('getSelected'));
		this.router.route(`/${controller.getParameters().modelname}/selectAll`).put(controller.execute('selectAll'));
		this.router.route(`/${controller.getParameters().modelname}/:id/select`).put(controller.execute('select'));
		this.router.route(`/${controller.getParameters().modelname}/clearSelection`).put(controller.execute('clearSelection'));
		this.router.route(`/${controller.getParameters().modelname}/deleteSelection`).delete(controller.execute('deleteSelection'));
		super.addController(controller);
	}
}
