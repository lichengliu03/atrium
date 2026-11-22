// Changing a message already sent, in place of it in the chat: the conversation goes back to just before it and
// carries on with what is sent from here (api.editMessage, sent by the chat: outbox). The files the agent changed
// since stay as they are.
import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { ArrowUp, X } from "lucide-react";
import type { Attachment, Upload } from "../api";
import { clsx } from "../util";
import { AttachButton, AttachmentTray, PendingUpload, useAttachments } from "./Composer";
import { pendingUploads } from "./outbox";

const UPLOADS = "/api/uploads/";

/** The message's attachments as the composer holds them: an upload is sent again as it is. */
function keptUploads(items: Attachment[]): PendingUpload[] {
  return items.flatMap((a, i) => {
    if (!a.url?.startsWith(UPLOADS)) return [];
    const id = decodeURIComponent(a.url.slice(UPLOADS.length));
    const upload = { id, name: a.name, path: a.path ?? id, mime: a.mime, size: 0, url: a.url };
    return [{ key: `kept-${i}`, name: a.name, mime: a.mime, preview: a.mime.startsWith("image/") ? a.url : undefined, upload }];
  });
}

export function MessageEditor({ text: sent, attachments, draft, later, running, onClose, onSend }: {
  text: string;
  attachments: Attachment[];
  draft?: { text: string; uploads: Upload[] } | null; // what was sent in its place and didn't go: written again
  later: number; // the user's messages after this one, which go too
  running: boolean;
  onClose: () => void;
  onSend: (text: string, uploads: Upload[]) => void;
}) {
  const [text, setText] = useState(draft?.text ?? sent);
  const held = useState<PendingUpload[]>(() => (draft ? pendingUploads(draft.uploads) : keptUploads(attachments)));
  const att = useAttachments(held);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  // an image the transcript holds itself (Claude's history) comes as an upload (written out when the history is read);
  // one still inline, which couldn't be written, is uploaded again; one with neither can't be sent again. Once: shown
  // again after its chat was kept hidden (App's ChatColumn), the effect runs again, and `held` already has them
  const [lost, setLost] = useState(0);
  const converted = useRef(false);
  useEffect(() => {
    if (converted.current) return;
    converted.current = true;
    const inline = draft ? [] : attachments.filter((a) => a.url?.startsWith("data:")); // a draft has its uploads
    setLost(attachments.filter((a) => !a.url?.startsWith(UPLOADS) && !a.url?.startsWith("data:")).length);
    void Promise.all(inline.map((a) => fetch(a.url!).then((r) => r.blob()).then((b) => new File([b], `${a.name}.${(b.type.split("/")[1] || "png").replace("jpeg", "jpg")}`, { type: b.type }))))
      .then((files) => files.length && att.add(files))
      .catch(() => setLost((n) => n + inline.length));
    // once, as it opens
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useLayoutEffect(() => {
    const el = inputRef.current;
    if (!el) return;
    el.style.height = "auto";
    el.style.height = Math.min(el.scrollHeight, Math.max(160, window.innerHeight * 0.5)) + "px";
  }, [text]);
  useEffect(() => {
    const el = inputRef.current;
    el?.focus();
    el?.setSelectionRange(el.value.length, el.value.length);
  }, []);

  const canSend = (!!text.trim() || att.ids.length > 0) && !att.uploading;
  // the chat takes it from here: the editor closes, and the message shows where it goes while it is sent
  const send = () => {
    if (canSend) onSend(text.trim(), att.items.flatMap((x) => (x.upload ? [x.upload] : [])));
  };

  const dropped = later ? `this message and the ${later === 1 ? "one" : later} after it are` : "this message is";
  return (
    <div className={clsx("composer-box msg-edit", att.dragging && "dragging")} {...att.dropProps}>
      {att.picker}
      <AttachmentTray items={att.items} onRemove={att.remove} />
      <textarea
        ref={inputRef}
        className="composer-input"
        rows={1}
        value={text}
        onChange={(e) => setText(e.target.value)}
        onPaste={att.onPaste}
        onKeyDown={(e) => {
          if (e.key === "Escape") {
            e.preventDefault();
            onClose();
          } else if (e.key === "Enter" && !e.shiftKey && !e.nativeEvent.isComposing) {
            e.preventDefault();
            send();
          }
        }}
      />
      <div className="msg-edit-hint">
        {running ? "The turn in progress stops, and the" : "The"} conversation goes back to here: {dropped} replaced by what you send. Files stay as
        they are.{lost ? ` ${lost === 1 ? "An attachment" : `${lost} attachments`} can't be sent again.` : ""}
      </div>
      <div className="composer-bar">
        <div className="bar-chips">
          <AttachButton onClick={att.open} />
        </div>
        <div className="bar-end">
          <button className="icon-btn" onClick={onClose} aria-label="Cancel" data-tip="Cancel (Esc)">
            <X size={16} />
          </button>
          <button className="send-btn" onClick={send} disabled={!canSend} aria-label="Send" data-tip="Send instead (Enter)">
            <ArrowUp size={17} strokeWidth={2.4} />
          </button>
        </div>
      </div>
    </div>
  );
}
