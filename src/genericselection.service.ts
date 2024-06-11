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
				case 'deleteSelection':
					return await this.deleteSelection(data);
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

	public async add(inputRequest: any): Promise<any> {
		try {
			const createData = await super.add(inputRequest);
			this.messageEvent.create(createData?.data);
			return createData;
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
		try {
			const dataToDelete = await this.model.findOne({ _id: inputRequest.params.id }).lean();
			const deleteData = await super.delete(inputRequest);
			await CRUDSelectionUtil.clearSelection(inputRequest?.query?.user, this.selectionModel, this.type);
			await this.messageEvent.delete(dataToDelete);
			return deleteData;
		} catch (err) {
			throw err;
		}
	}

	public async deleteSelection(inputRequest: any): Promise<any> {
		try {
			const selectedData = await this.getSelected(inputRequest);
			const listData = selectedData?.data;
			await Promise.all(
				listData.map(async (line: any) => {
					await this.model.deleteOne({ _id: line?._id });
					await this.messageEvent.delete(line);
				})
			);
			await CRUDSelectionUtil.clearSelection(inputRequest?.query?.user, this.selectionModel, this.type);
			return selectedData;
		} catch (err) {
			throw err;
		}
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
}
