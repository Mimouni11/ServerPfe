package com.example.pfemini;

import android.content.Context;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.ImageView;

public class AvatarAdapter extends BaseAdapter {
    private Context context;
    private int[] avatarList; // Array of avatar resource IDs

    public AvatarAdapter(Context context, int[] avatarList) {
        this.context = context;
        this.avatarList = avatarList;
    }

    @Override
    public int getCount() {
        return avatarList.length;
    }

    @Override
    public Object getItem(int position) {
        return avatarList[position];
    }

    @Override
    public long getItemId(int position) {
        return position;
    }

    @Override
    public View getView(int position, View convertView, ViewGroup parent) {
        if (convertView == null) {
            convertView = LayoutInflater.from(context).inflate(R.layout.avatar_item_layout, parent, false);
        }

        ImageView imageView = convertView.findViewById(R.id.avatarImageView);
        imageView.setImageResource(avatarList[position]);

        return convertView;
    }
}
