export type ClientDlrStatus =
  | "success"
  | "failed"
  | "unknown";

export interface ClientDlr {
  messageId: string;
  providerMessageId: string;
  status: ClientDlrStatus;
}