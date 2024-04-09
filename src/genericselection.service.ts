import { MongoDBService } from "@3kles/3kles-coremongodb";
import mongoose from 'mongoose';
import { GenericCRUDEvent } from "./generic.crud.event";
import { DBUtils } from "./db.util";
import { CRUDSelectionUtil } from "./crudselection.util";

export class MongoDBSelectionService extends MongoDBService {

	constructor(public model: mongoose.Model<mongoose.Document>,
		public selectionModel: mongoose.Model<mongoose.Document>,
		public type: string,
		public defaultSort: string,
		public messageEvent: GenericCRUDEvent,
		public parentKey?: string) {
		super(model);
	}

	public async execute(type: string, data: any): Promise<any> {
		try {
			switch (type) {
				case 'getSelected':
					return await this.getSelected(data);
				case 'selectAll':
					return await this.selectAll(data);
				case 'select':
					return await this.select(data);
				case 'clearSelection':
					return await this.clearSelection(data);
			}
			return super.execute(type, data);
		} catch (e) {
			console.error(e);
			throw e;
		}
	}

	public async list(inputRequest: any): Promise<any> {
		let filter: any = {};
		if (inputRequest.headers.filter) {
			filter = JSON.parse(inputRequest.headers.filter);
		}
		filter = DBUtils.formatMongoDBFilter(filter);
		try {
			const userid = new mongoose.Types.ObjectId(inputRequest?.query?.user);
			const records = await this.model.aggregate([
				{ $match: filter },
				...DBUtils.skipLimitSortPipeLine(inputRequest, this.defaultSort),
				...CRUDSelectionUtil.userSelectionPipeline(this.type, userid),
			]);
			const countAllLines = await this.model.count();
			const countSelection = await this.selectionModel.count({
				type: this.type,
				user: userid
			});
			return {
				data: {
					header: {
						'#select': !!(countAllLines === countSelection && countSelection),
						indeterminate: !!(countAllLines !== countSelection && countSelection),
						countSelection
					},
					lines: records
				},
				totalCount: await this.model.count({ ...filter })
			};
		} catch (err) {
			throw err;
		}
	}

	public async update(inputRequest: any): Promise<any> {
		await super.update(inputRequest);
		const updateData = await this.model.findOne({ _id: inputRequest.params.id }).lean();
		this.messageEvent.update(updateData);
		return { data: updateData };
	}

	public async delete(inputRequest: any): Promise<any> {
		const data = await super.delete(inputRequest);
		await CRUDSelectionUtil.clearSelection(inputRequest?.query?.user, this.selectionModel, this.type);
		return { data };
	}

	public async getSelected(inputRequest: any): Promise<any> {
		try {
			const userid = new mongoose.Types.ObjectId(inputRequest?.query?.user);
			const records = await this.model.aggregate([
				...CRUDSelectionUtil.userSelectionPipeline(this.type, userid),
				{
					$match: {
						'#select': true
					}
				},
			]);
			return {
				data: records,
				totalCount: records.length
			};
		} catch (err) {
			throw err;
		}
	}

	public async select(data: any): Promise<any> {
		return await CRUDSelectionUtil.select(data, this.model, this.selectionModel, this.type, this.parentKey);
	}

	public async selectAll(data: any): Promise<any> {
		return await CRUDSelectionUtil.selectAll(data, this.model, this.selectionModel, this.type);
	}

	public async clearSelection(data: any): Promise<any> {
		return await CRUDSelectionUtil.clearSelection(data.body?.user, this.selectionModel, this.type);
	}

	/** Children selection */
	public async createParentSelection(line: any, user: any, list?: any[]): Promise<any[]> {
		if (!list) { list = []; }
		const parentLine = await this.model.findById(line.parent).lean();
		// check if parentLine has another child not selected (indeterminate)
		const listOtherChildren = await this.model.find({ parent: line.parent, _id: { $ne: line?._id } });
		const indeterminate = await this.selectionModel.find({ selection: { $in: listOtherChildren.map((m) => m._id) } });
		if (parentLine) {
			list.push(
				{
					updateOne: {
						filter: { selection: parentLine._id },
						update: {
							$set: {
								selection: parentLine._id,
								indeterminate: !!(listOtherChildren.length !== indeterminate.length),
								user,
								type: this.type
							}
						},
						upsert: true,
					},
				}
			);
			return await this.createParentSelection(parentLine, user, list);
		}
		return list;
	}

	public async createChildrenSelection(line: any, user: any, list?: any[]): Promise<any[]> {
		if (!list) { list = []; }
		const childrenLines = await this.model.find({ parent: line._id }).lean();
		if (childrenLines.length) {
			await Promise.all(
				childrenLines.map(async (children) => {
					list.push({
						updateOne: {
							filter: { selection: children._id },
							update: {
								$set: {
									selection: children._id,
									user,
									type: this.type
								}
							},
							upsert: true,
						},
					});
					return this.createChildrenSelection(children, user, list);
				})
			);
		}
		return list;
	}

	public async deleteParentSelection(line: any, user: any, list?: any[]): Promise<any[]> {
		if (!list) { list = []; }
		const parentLine = await this.model.findById(line.parent).lean();
		// check if parentLine has another child not selected (indeterminate)
		const listOtherChildren = await this.model.find({ parent: line.parent, _id: { $ne: line?._id } });
		const indeterminate = !!await this.selectionModel.countDocuments({ selection: { $nin: listOtherChildren.map((m) => m._id) } });
		if (parentLine) {
			if (listOtherChildren.length) {
				list.push(
					{
						updateOne: {
							filter: { selection: parentLine._id },
							update: {
								$set: {
									selection: parentLine._id,
									indeterminate,
									user,
									type: this.type
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
			return await this.createParentSelection(parentLine, user, list);
		}
		return list;
	}


	public async deleteChildrenSelection(line: any, user: any, list?: any[]): Promise<any[]> {
		if (!list) { list = []; }
		const childrenLines = await this.model.find({ parent: line._id }).lean();
		if (childrenLines.length) {
			await Promise.all(
				childrenLines.map(async (children) => {
					list.push({
						deleteOne: {
							filter: { selection: children._id }
						},
					});
					return this.deleteChildrenSelection(children, user, list);
				})
			);
		}
		return list;
	}
}
