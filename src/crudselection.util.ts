import { MessageBroker } from "@3kles/3kles-amqpbroker";
import { CRUDUtil, GenericRouter } from "@3kles/3kles-corebe";
import { MongoDBController } from "@3kles/3kles-coremongodb";
import mongoose from "mongoose";
import { GenericCRUDEvent } from "./generic.crud.event";
import { MongoDBSelectionRouter } from "./genericselection.route";
import { MongoDBSelectionService } from "./genericselection.service";
import { DBUtils } from "./db.util";

export class CRUDSelectionUtil extends CRUDUtil {

	public static buildGenericRoute(
		idApp: string,
		broker: MessageBroker,
		model: mongoose.Model<mongoose.Document>,
		selectionModel: mongoose.Model<mongoose.Document>,
		type: string, defaultSort: string): GenericRouter {
		const messageEvent = new GenericCRUDEvent(broker, idApp, type);
		const service = new MongoDBSelectionService(model, selectionModel, type, defaultSort, messageEvent);
		const router = new MongoDBSelectionRouter(new MongoDBController(service));
		return router;
	}

	public static generate(route: string, hostname: string, port: number, path: string, headerKeys?: string[], middlewares?: any[]): { [key: string]: any; } {
		route += route.endsWith("/") ? "" : "/";
		const option = {
			hostname,
			port,
			headers: {
				'Content-Type': 'application/json',
			}
		};
		return {
			...CRUDUtil.generate(route, hostname, port, path, headerKeys, middlewares),
			select: {
				method: 'PUT',
				path: ':id/select',
				headerKeys: headerKeys || [],
				option: {
					path: path + '/:id/select',
					method: 'PUT',
					...option
				},
				middlewares: middlewares || []
			},
			selectAll: {
				method: 'PUT',
				path: route + 'selectAll',
				headerKeys: headerKeys || [],
				option: {
					path: path + '/selectAll',
					method: 'PUT',
					...option
				},
				middlewares: middlewares || []
			},
			clearSelection: {
				method: 'PUT',
				path: route + 'clearSelection',
				headerKeys: headerKeys || [],
				option: {
					path: path + '/clearSelection',
					method: 'PUT',
					...option
				},
				middlewares: middlewares || []
			}
		};
	}

	public static async select(inputRequest: any, model: mongoose.Model<any>, selectionModel: mongoose.Model<any>, type: string, parentKey?: string): Promise<any> {
		try {
			const user = inputRequest.body?.user;
			const line: any = await model.findById(inputRequest.params?.id).lean();
			const userSelectLine: any = await selectionModel.findOne({
				type,
				selection: line?._id,
				user
			}).lean();

			let children = [];
			if (userSelectLine) {
				await selectionModel.deleteOne({ _id: userSelectLine._id });
				if (parentKey) {
					// check if has children (line is a parent)
					if (await model.countDocuments({ [parentKey]: line._id })) {
						const listBulkChildren = (await CRUDSelectionUtil.deleteChildrenSelection(model, line, user, parentKey)).flat();
						children = children.concat(listBulkChildren.map((m) => ({
							_id: m.deleteOne?.filter?.selection,
							selected: false
						})));
						await selectionModel.bulkWrite(listBulkChildren);
					}
					// If is child (has parent)
					if (line[parentKey]) {
						const listBulkParent = await CRUDSelectionUtil.deleteParentSelection(model, selectionModel, type, line, user, parentKey);
						children = children.concat(listBulkParent.map((m) => ({
							_id: (m.updateOne) ? m.updateOne?.update.$set?.selection : m.deleteOne?.filter?.selection,
							selected: false,
							indeterminate: (m.updateOne) ? m.updateOne?.update.$set?.indeterminate : false,
						})));
						await selectionModel.bulkWrite(listBulkParent);
					}
				}
			} else {
				await selectionModel.create({
					type,
					selection: line._id,
					[parentKey]: line?.[parentKey],
					user
				});
				if (parentKey) {
					// If is child (has parent)
					if (line[parentKey]) {
						const listBulkParent = await CRUDSelectionUtil.createParentSelection(model, selectionModel, type, line, user, parentKey);
						children = children.concat(listBulkParent.map((m) => ({
							_id: m.updateOne?.update.$set?.selection,
							selected: true,
							indeterminate: m.updateOne?.update.$set?.indeterminate,
						})));
						await selectionModel.bulkWrite(listBulkParent);
					}
					// check if has children (line is a parent)
					if (await model.countDocuments({ [parentKey]: line._id })) {
						const listBulkChildren = (await CRUDSelectionUtil.createChildrenSelection(model, selectionModel, type, line, user, parentKey)).flat();
						children = children.concat(listBulkChildren.map((m) => ({
							_id: m.updateOne?.update.$set?.selection,
							selected: true,
							indeterminate: m.updateOne?.update.$set?.indeterminate,
						})));
						await selectionModel.bulkWrite(listBulkChildren);
					}
				}
			}
			const countSelection = await selectionModel.count({
				type,
				user
			});
			const nbLine = await model.count();
			return {
				data: {
					'indeterminate': !!(+countSelection !== +nbLine && countSelection),
					'#select': !!(+countSelection === +nbLine && countSelection),
					countSelection,
					children
				}
			};
		} catch (err) {
			console.error(err);
		}
	}

	public static async selectAll(inputRequest: any, model: mongoose.Model<any>, selectionModel: mongoose.Model<any>, type: string): Promise<any> {
		try {
			let filter: any = {};
			if (inputRequest.headers.filter) {
				filter = JSON.parse(inputRequest.headers.filter);
			}
			filter = DBUtils.formatMongoDBFilter(filter);
			const user = inputRequest.body?.user;
			const counttemplates = await model.count();
			await selectionModel.deleteMany({
				type,
				user
			});

			const selected = !!inputRequest.body?.selected;
			if (selected) {
				const listLinesToSelect = await model.find({ ...filter }).lean();
				await selectionModel.insertMany(listLinesToSelect.map((line) => {
					return {
						type,
						user,
						selection: line?._id
					};
				}));
			}

			const countSelection = await selectionModel.count({
				type,
				user
			});

			return {
				data: {
					all: !!(+counttemplates === +countSelection && countSelection),
					selected,
					indeterminate: !!(+counttemplates !== +countSelection && countSelection),
					countSelection
				}
			};
		} catch (err) {
			console.error(err);
		}
	}

	public static async clearSelection(user: string, selectionModel: mongoose.Model<any>, type: string): Promise<any> {
		try {
			return {
				data: await selectionModel.deleteMany({
					type,
					user
				})
			};
		} catch (err) {
			console.error(err);
		}
	}

	/** Children selection */
	public static async createParentSelection(model: mongoose.Model<any>, selectionModel: mongoose.Model<any>, type: string,
		line: any, user: any, parentKey: string, list?: any[], forceIndeterminate?: boolean): Promise<any[]> {
		if (!list) { list = []; }
		const parentLine = await model.findById(line[parentKey]).lean<any>();
		// check if parentLine has another child not selected (indeterminate)
		const listOtherChildren = await model.find({ [parentKey]: line[parentKey], _id: { $ne: line?._id } }).lean<any[]>();
		const listOtherChildrenSelected = await selectionModel.find({ [parentKey]: line[parentKey], selection: { $in: listOtherChildren.map((m) => m._id) } }).lean<any[]>();
		if (parentLine) {
			const indeterminate = !!(listOtherChildren.length !== listOtherChildrenSelected.length) || forceIndeterminate;
			list.push(
				{
					updateOne: {
						filter: { selection: parentLine._id },
						update: {
							$set: {
								selection: parentLine._id,
								[parentKey]: parentLine?.[parentKey],
								indeterminate,
								user,
								type
							}
						},
						upsert: true,
					},
				}
			);
			return await CRUDSelectionUtil.createParentSelection(model, selectionModel, type, parentLine, user, parentKey, list, indeterminate);
		}
		return list;
	}

	public static async createChildrenSelection(model: mongoose.Model<any>, selectionModel: mongoose.Model<any>, type: string,
		line: any, user: any, parentKey: string, list?: any[]): Promise<any[]> {
		if (!list) { list = []; }
		const childrenLines = await model.find({ [parentKey]: line._id }).lean();
		if (childrenLines.length) {
			await Promise.all(
				childrenLines.map(async (children) => {
					list.push({
						updateOne: {
							filter: { selection: children._id },
							update: {
								$set: {
									selection: children._id,
									[parentKey]: children[parentKey],
									user,
									type
								}
							},
							upsert: true,
						},
					});
					return CRUDSelectionUtil.createChildrenSelection(model, selectionModel, type, children, user, parentKey, list);
				})
			);
		}
		return list;
	}

	/** Unselect parent */
	public static async deleteParentSelection(model: mongoose.Model<any>, selectionModel: mongoose.Model<any>, type: string,
		line: any, user: any, parentKey: string, list?: any[], forceIndeterminate?: boolean): Promise<any[]> {
		if (!list) { list = []; }
		const parentLine = await model.findById(line[parentKey]).lean<any>();

		if (parentLine) {
			// check if parentLine has another child not selected (indeterminate)
			const listOtherChildren = await model.find({ [parentKey]: line[parentKey], _id: { $ne: line?._id } }).lean<any[]>();
			const countOtherChildrenSelected = !!await selectionModel.countDocuments({
				[parentKey]: line[parentKey],
				selection: { $in: listOtherChildren.map((m) => m._id) }
			}).lean<any[]>();
			const indeterminate = (!!countOtherChildrenSelected || forceIndeterminate);

			if (countOtherChildrenSelected || forceIndeterminate) {
				list.push(
					{
						updateOne: {
							filter: { selection: parentLine._id },
							update: {
								$set: {
									selection: parentLine._id,
									[parentKey]: parentLine[parentKey],
									indeterminate,
									user,
									type
								}
							},
							upsert: true,
						},
					}
				);
			} else {
				list.push(
					{
						deleteOne: {
							filter: { selection: parentLine._id }
						}
					}
				);
			}
			return await CRUDSelectionUtil.deleteParentSelection(model, selectionModel, type, parentLine, user, parentKey, list, indeterminate);
		}
		return list;
	}


	/** Unselect children */
	public static async deleteChildrenSelection(model: mongoose.Model<any>, line: any, user: any, parentKey: string, list?: any[]): Promise<any[]> {
		if (!list) { list = []; }
		const childrenLines = await model.find({ [parentKey]: line._id }).lean();
		if (childrenLines.length) {
			await Promise.all(
				childrenLines.map(async (children) => {
					list.push({
						deleteOne: {
							filter: { selection: children._id }
						},
					});
					return CRUDSelectionUtil.deleteChildrenSelection(model, children, user, parentKey, list);
				})
			);
		}
		return list;
	}

	public static userSelectionPipeline(type: string, user: any): any[] {
		return [
			{
				$lookup: {
					from: "userselections",
					let: {
						id: "$_id"
					},
					pipeline: [
						{
							$match: {
								$expr: {
									$and: [
										{
											$eq: [
												"$user",
												user
											]
										},
										{
											$eq: [
												"$$id",
												"$selection"
											]
										},
										{
											$eq: [
												"$type",
												type
											]
										}
									]
								}
							}
						}
					],
					as: "userselection"
				}
			},
			{
				$addFields: {
					"#select": {
						$toBool: {
							$size: "$userselection"
						}
					}
				}
			},
			{
				$unwind: {
					path: "$userselection",
					preserveNullAndEmptyArrays: true
				}
			},
			{
				$addFields: {
					indeterminate: "$userselection.indeterminate"
				}
			},
			{
				$unset: [
					"userselection",
				]
			}
		];
	}

}
