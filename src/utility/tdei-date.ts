export class TdeiDate {

    public static UTC(date?: string | Date): string {
        if (!date) return (new Date()).toISOString();
        return (new Date(date)).toISOString();
    }

    public static isValid(dateStr: any): boolean {
        try {
            const date = new Date(dateStr);

            const timestamp = date.getTime();

            if (typeof timestamp !== 'number' || Number.isNaN(timestamp)) {
                return false;
            }
        } catch (error) {
            return false;
        }
        return true;
    }
}