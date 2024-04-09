import { MessageBroker } from "@3kles/3kles-amqpbroker";

export class GenericCRUDEvent {

	constructor(public broker: MessageBroker, public idApp: string, public type: string) { }

	public async create(data: any): Promise<void> {
		await this.emit(data, `${this.type}.insert`);
	}

	public async update(data: any): Promise<void> {
		await this.emit(data, `${this.type}.update`);
	}

	public async delete(data: any): Promise<void> {
		await this.emit(data, `${this.type}.delete`);
	}

	protected async emit(data: any, type: string): Promise<void> {
		try {
			await this.broker.sendToExchange(`${this.idApp}.event`, `${this.type}.event`,
				Buffer.from(JSON.stringify(await this.getMessage(data, type))),
				'topic',
				{ durable: false });
		} catch (error) {
			console.error(error);
		}
	}

	public async getMessage(data: any, type: string): Promise<IMessage> {
		return {
			type,
			content: data
		};
	}
}

export interface IMessage {
	type: string;
	content: any;
	room?: any;
}
