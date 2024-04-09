import mongoose from 'mongoose';

export class DBUtils {

	public static formatMongoDBFilter(filter: any): any {
		return Object.keys(filter)
			.map((key) => {
				if (Array.isArray(filter[key]) && !key.startsWith('$')) {
					return { [key]: { $in: filter[key] } };
				}
				if (typeof filter[key] === 'string') {
					// if (DateTime.fromISO(filter[key]).isValid && +filter[key]?.length >= 8) {
					//     return { [key]: { $gte: DateTime.fromISO(filter[key]).toJSDate(), $lt: DateTime.fromISO(filter[key]).plus({ days: 1 }).toJSDate() } };
					// }
					if (mongoose.isValidObjectId(filter[key])) {
						return { [key]: new mongoose.Types.ObjectId(filter[key]) };
					}
					return { [key]: { $regex: filter[key], $options: "i" } };
				}
				return { [key]: filter[key] };
			}).reduce((a, b) => ({ ...a, ...b }), {});
	}

	public static skipLimitSortPipeLine(inputRequest: any, defaultSort: string): any[] {
		return [
			{
				$sort: ((inputRequest.query?.sort && inputRequest.query?.order) ?
					{ [inputRequest.query?.sort]: (inputRequest.query?.order === 'asc') ? 1 : -1 }
					: { [defaultSort]: 1 })
			},
			{
				$skip: ((+inputRequest.query.page - 1) * +inputRequest.query.per_page)
			},
			{
				$limit: (+inputRequest.query.per_page)
			}
		];
	}

	public static lookupPipeline(from: string, localField: string, foreignField: string, as: string): any {
		return [{
			$lookup:
			{
				from,
				localField,
				foreignField,
				as
			}
		},
		{
			$unwind: {
				path: `$${as}`,
				preserveNullAndEmptyArrays: true
			}
		}];
	}
}
